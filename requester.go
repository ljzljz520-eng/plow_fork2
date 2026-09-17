package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"math"
	"net"
	url2 "net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/valyala/fasthttp"
	"github.com/valyala/fasthttp/fasthttpproxy"
	"go.uber.org/automaxprocs/maxprocs"
	"golang.org/x/time/rate"
)

var (
	startTimeUnixNano int64
	sendOnCloseError  interface{}
)

type ReportRecord struct {
	cost             time.Duration
	code             int
	error            string
	readBytes        int64
	writeBytes       int64
	concurrencyCount int
}

var recordPool = sync.Pool{
	New: func() interface{} { return new(ReportRecord) },
}

func init() {
	// Honoring env GOMAXPROCS
	_, _ = maxprocs.Set()
	defer func() {
		sendOnCloseError = recover()
	}()
	func() {
		cc := make(chan struct{}, 1)
		close(cc)
		cc <- struct{}{}
	}()
}

type MyConn struct {
	net.Conn
	r, w *int64
}

func NewMyConn(conn net.Conn, r, w *int64) (*MyConn, error) {
	myConn := &MyConn{Conn: conn, r: r, w: w}
	return myConn, nil
}

func (c *MyConn) Read(b []byte) (n int, err error) {
	sz, err := c.Conn.Read(b)

	if err == nil {
		atomic.AddInt64(c.r, int64(sz))
	}
	return sz, err
}

func (c *MyConn) Write(b []byte) (n int, err error) {
	sz, err := c.Conn.Write(b)

	if err == nil {
		atomic.AddInt64(c.w, int64(sz))
	}
	return sz, err
}

func ThroughputInterceptorDial(dial fasthttp.DialFunc, r *int64, w *int64) fasthttp.DialFunc {
	return func(addr string) (net.Conn, error) {
		conn, err := dial(addr)
		if err != nil {
			return nil, err
		}
		return NewMyConn(conn, r, w)
	}
}

type Requester struct {
	concurrency int
	reqRate     *rate.Limit
	requests    int64
	duration    time.Duration
	rampUp      int
	clientOpt   *ClientOpt
	httpClient  *fasthttp.HostClient
	httpHeader  *fasthttp.RequestHeader
	errWriter   io.Writer

	recordChan chan *ReportRecord
	closeOnce  sync.Once
	wg         sync.WaitGroup

	readBytes  int64
	writeBytes int64

	// Distributed scheduling. When startAt/stopAt are set, Run waits for the
	// exact absolute instant instead of starting immediately, allowing every
	// cluster node to fire at the same controller-scheduled moment.
	startAt time.Time
	stopAt  time.Time

	// Remaining request quota, mutated atomically when the controller
	// rebalances budgets across live nodes.
	quota        atomic.Int64
	quotaEnabled atomic.Bool

	// Dynamic per-node RPS; swapped at runtime by set_rate commands.
	rateLimiter atomic.Pointer[rate.Limiter]

	// Created in the constructor so external Cancel calls are safe at any
	// time, including the window before Run starts, where a cluster stop or
	// abort may race the prepare command.
	ctx    context.Context
	cancel func()
}

// SetSchedule pins the run to absolute local-clock instants. All cluster
// nodes receive the same controller-domain times converted with their clock
// offset, so traffic starts/stops simultaneously regardless of clock skew.
func (r *Requester) SetSchedule(startAt, stopAt time.Time) {
	r.startAt = startAt
	r.stopAt = stopAt
}

// SetRate dynamically updates the node RPS limit (nil means unlimited).
func (r *Requester) SetRate(l *rate.Limiter) {
	r.rateLimiter.Store(l)
}

// SetQuota replaces the remaining request budget of this node. A negative
// value means unlimited. Used when the controller redistributes the share of
// a node that left or died.
func (r *Requester) SetQuota(n int64) {
	if n < 0 {
		r.quotaEnabled.Store(false)
		return
	}
	r.quota.Store(n)
	r.quotaEnabled.Store(true)
}

type ClientOpt struct {
	url       string
	method    string
	headers   []string
	bodyBytes []byte
	bodyFile  string

	certPath string
	keyPath  string
	insecure bool

	maxConns     int
	doTimeout    time.Duration
	readTimeout  time.Duration
	writeTimeout time.Duration
	dialTimeout  time.Duration

	socks5Proxy string
	httpProxy   string
	contentType string
	host        string
	unixSocket  string
}

func NewRequester(concurrency int, requests int64, duration time.Duration, reqRate *rate.Limit, errWriter io.Writer, clientOpt *ClientOpt, rampUp int) (*Requester, error) {
	maxResult := concurrency * 100
	if maxResult > 8192 {
		maxResult = 8192
	}
	r := &Requester{
		concurrency: concurrency,
		reqRate:     reqRate,
		requests:    requests,
		duration:    duration,
		rampUp:      rampUp,
		errWriter:   errWriter,
		clientOpt:   clientOpt,
		recordChan:  make(chan *ReportRecord, maxResult),
	}
	if requests > 0 {
		r.quota.Store(requests)
		r.quotaEnabled.Store(true)
	}
	r.ctx, r.cancel = context.WithCancel(context.Background())
	client, header, err := buildRequestClient(clientOpt, &r.readBytes, &r.writeBytes)
	if err != nil {
		return nil, err
	}
	r.httpClient = client
	r.httpHeader = header
	return r, nil
}

func addMissingPort(addr string, isTLS bool) string {
	if _, _, err := net.SplitHostPort(addr); err == nil {
		return addr
	}
	port := 80
	if isTLS {
		port = 443
	}
	return net.JoinHostPort(strings.Trim(addr, "[]"), strconv.Itoa(port))
}

func buildTLSConfig(opt *ClientOpt) (*tls.Config, error) {
	var certs []tls.Certificate
	if opt.certPath != "" && opt.keyPath != "" {
		c, err := tls.LoadX509KeyPair(opt.certPath, opt.keyPath)
		if err != nil {
			return nil, err
		}
		certs = append(certs, c)
	}
	return &tls.Config{
		InsecureSkipVerify: opt.insecure,
		Certificates:       certs,
	}, nil
}

func buildRequestClient(opt *ClientOpt, r *int64, w *int64) (*fasthttp.HostClient, *fasthttp.RequestHeader, error) {
	u, err := url2.Parse(opt.url)
	if err != nil {
		return nil, nil, err
	}
	httpClient := &fasthttp.HostClient{
		Addr:                          addMissingPort(u.Host, u.Scheme == "https"),
		IsTLS:                         u.Scheme == "https",
		Name:                          "plow",
		MaxConns:                      opt.maxConns,
		ReadTimeout:                   opt.readTimeout,
		WriteTimeout:                  opt.writeTimeout,
		DisableHeaderNamesNormalizing: true,
	}
	if opt.socks5Proxy != "" {
		if !strings.Contains(opt.socks5Proxy, "://") {
			opt.socks5Proxy = "socks5://" + opt.socks5Proxy
		}
		httpClient.Dial = fasthttpproxy.FasthttpSocksDialer(opt.socks5Proxy)
	} else if opt.unixSocket != "" {
		httpClient.Dial = func(addr string) (net.Conn, error) {
			return net.Dial("unix", opt.unixSocket)
		}
	} else if opt.httpProxy != "" {
		httpClient.Dial = fasthttpproxy.FasthttpHTTPDialerDualStack(opt.httpProxy)
	} else {
		dialer := fasthttpproxy.Dialer{
			Timeout:        opt.dialTimeout,
			ConnectTimeout: opt.dialTimeout,
			DialDualStack:  true,
		}
		httpClient.Dial, err = dialer.GetDialFunc(true)
		if err != nil {
			return nil, nil, err
		}
	}
	httpClient.Dial = ThroughputInterceptorDial(httpClient.Dial, r, w)

	tlsConfig, err := buildTLSConfig(opt)
	if err != nil {
		return nil, nil, err
	}
	httpClient.TLSConfig = tlsConfig

	var requestHeader fasthttp.RequestHeader
	if opt.contentType != "" {
		requestHeader.SetContentType(opt.contentType)
	}
	if opt.host != "" {
		requestHeader.SetHost(opt.host)
	} else {
		requestHeader.SetHost(u.Host)
	}
	requestHeader.SetMethod(opt.method)
	requestHeader.SetRequestURI(u.RequestURI())
	for _, h := range opt.headers {
		n := strings.SplitN(h, ":", 2)
		if len(n) != 2 {
			return nil, nil, fmt.Errorf("invalid header: %s", h)
		}
		requestHeader.Set(strings.TrimSpace(n[0]), strings.TrimSpace(n[1]))
	}

	return httpClient, &requestHeader, nil
}

func (r *Requester) Cancel() {
	r.cancel()
}

func (r *Requester) RecordChan() <-chan *ReportRecord {
	return r.recordChan
}

func (r *Requester) closeRecord() {
	r.closeOnce.Do(func() {
		close(r.recordChan)
	})
}

// emitRecord delivers a sample, bailing out once the run is canceled. The
// channel is closed only after every worker has returned (Run closes it past
// wg.Wait), so send and close can never race; the ctx guard additionally
// prevents a worker from blocking on a full buffer after stop.
func (r *Requester) emitRecord(ctx context.Context, rr *ReportRecord) {
	select {
	case r.recordChan <- rr:
	case <-ctx.Done():
		recordPool.Put(rr)
	}
}

func (r *Requester) DoRequest(req *fasthttp.Request, resp *fasthttp.Response, rr *ReportRecord) {
	startTime := time.Unix(0, atomic.LoadInt64(&startTimeUnixNano))
	t1 := time.Since(startTime)
	var err error
	if r.clientOpt.doTimeout > 0 {
		err = r.httpClient.DoTimeout(req, resp, r.clientOpt.doTimeout)
	} else {
		err = r.httpClient.Do(req, resp)
	}

	if err != nil {
		rr.cost = time.Since(startTime) - t1
		rr.error = err.Error()
		return
	}

	writeTo := io.Discard
	if resp.StatusCode() >= 500 {
		writeTo = r.errWriter
		_, _ = r.errWriter.Write([]byte(fmt.Sprintf("\n%d %s\n", resp.StatusCode(), rr.cost)))
		_, _ = r.errWriter.Write([]byte(fmt.Sprintf("%s", &resp.Header)))
	}
	err = resp.BodyWriteTo(writeTo)
	if err != nil {
		rr.cost = time.Since(startTime) - t1
		rr.error = err.Error()
		return
	}

	rr.cost = time.Since(startTime) - t1
	rr.code = resp.StatusCode()
	rr.error = ""
}

func (r *Requester) Run() {
	// handle ctrl-c
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, os.Interrupt, syscall.SIGTERM)

	ctx, cancelFunc := r.ctx, r.cancel
	signalDone := make(chan struct{})
	go func() {
		defer close(signalDone)
		select {
		case <-sigs:
			// Signal workers to stop; recordChan is closed only after every
			// sender has exited, so a signal can never race a pending send.
			cancelFunc()
		case <-ctx.Done():
		}
	}()
	defer func() {
		signal.Stop(sigs)
		cancelFunc()
		<-signalDone
	}()
	// Wait for the synchronized absolute start instant when running under
	// the cluster controller; otherwise start immediately.
	start := time.Now()
	if !r.startAt.IsZero() {
		wait := time.Until(r.startAt)
		if wait > 0 {
			timer := time.NewTimer(wait)
			select {
			case <-ctx.Done():
				return
			case <-timer.C:
			}
		}
		// Anchor stats to the scheduled instant on every node, even if the
		// goroutine woke up a little late.
		start = r.startAt
	}
	atomic.StoreInt64(&startTimeUnixNano, start.UnixNano())

	// Stop at the absolute stop instant (cluster) or after duration (local).
	stopAt := r.stopAt
	if stopAt.IsZero() && r.duration > 0 {
		stopAt = start.Add(r.duration)
	}
	if !stopAt.IsZero() && stopAt.After(start) {
		d := time.Until(stopAt)
		if d < 0 {
			d = 0
		}
		time.AfterFunc(d, cancelFunc)
	}

	// The shared limiter is always read through an atomic pointer so a
	// cluster controller can swap the per-node RPS at runtime (set_rate).
	if r.reqRate != nil {
		r.rateLimiter.Store(rate.NewLimiter(*r.reqRate, 1))
	}

	if r.rampUp <= 0 {
		r.rampUp = r.concurrency
	}
	concurrencyCount := 0
	loopCount := int(math.Ceil(float64(r.concurrency) / float64(r.rampUp)))
	for i := 0; i < loopCount; i++ {
		for j := 0; j < r.rampUp; j++ {
			if concurrencyCount >= r.concurrency {
				break
			}
			concurrencyCount++
			workerID := concurrencyCount
			r.wg.Add(1)
			go func(concurrencyCount int) {
				defer func() {
					r.wg.Done()
					v := recover()
					if v != nil && v != sendOnCloseError {
						panic(v)
					}
				}()
				req := &fasthttp.Request{}
				resp := &fasthttp.Response{}
				r.httpHeader.CopyTo(&req.Header)
				if r.httpClient.IsTLS {
					req.URI().SetScheme("https")
					req.URI().SetHostBytes(req.Header.Host())
				}

				for {
					select {
					case <-ctx.Done():
						return
					default:
					}

					if limiter := r.rateLimiter.Load(); limiter != nil {
						err := limiter.Wait(ctx)
						if err != nil {
							continue
						}
					}

					if r.quotaEnabled.Load() && r.quota.Add(-1) < 0 {
						// This node's budget is spent. Do not cancel the shared
						// context: peers may be finishing a quota-charged,
						// in-flight request whose record must still be counted.
						// They independently fail their next token check.
						return
					}

					if r.clientOpt.bodyFile != "" {
						file, err := os.Open(r.clientOpt.bodyFile)
						if err != nil {
							rr := recordPool.Get().(*ReportRecord)
							rr.cost = 0
							rr.error = err.Error()
							rr.readBytes = atomic.LoadInt64(&r.readBytes)
							rr.writeBytes = atomic.LoadInt64(&r.writeBytes)
							rr.concurrencyCount = concurrencyCount
							r.emitRecord(ctx, rr)
							continue
						}
						req.SetBodyStream(file, -1)
					} else {
						req.SetBodyRaw(r.clientOpt.bodyBytes)
					}
					resp.Reset()
					rr := recordPool.Get().(*ReportRecord)
					r.DoRequest(req, resp, rr)
					rr.readBytes = atomic.LoadInt64(&r.readBytes)
					rr.writeBytes = atomic.LoadInt64(&r.writeBytes)
					rr.concurrencyCount = concurrencyCount
					r.emitRecord(ctx, rr)
				}
			}(workerID)
		}
		// Wait between ramp-up batches, but never after the last one so a
		// node whose assigned concurrency is below the ramp-up size exits
		// promptly once its quota is exhausted.
		if i < loopCount-1 {
			time.Sleep(time.Second)
		}
	}

	r.wg.Wait()
	r.closeRecord()
}
