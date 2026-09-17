package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

// Agent is a load-generating node. It registers once, heartbeats on an
// interval (retries keep the same seq so stats are deduplicated), applies
// epoch-ordered commands, converts controller-domain schedule times to its
// local clock using the measured offset, and streams cumulative stats.

type Agent struct {
	baseURL string
	id      string
	host    string
	weight  float64

	httpc *http.Client
	clock *clockState

	interval time.Duration
	timeout  time.Duration
	session  string

	mu        sync.Mutex
	phase     string
	applied   int64
	requester *Requester
	collector *nodeCollector
	startAt   time.Time
	abortErr  error

	// crashOnStop simulates a hard node crash: context cancellation stops
	// heartbeats without sending deregister, exercising failure detection.
	crashOnStop bool
}

func newAgent(baseURL, id, hostOverride string, weight float64) (*Agent, error) {
	host, err := os.Hostname()
	if err != nil {
		host = id
	}
	if hostOverride != "" {
		host = hostOverride
	}
	if id == "" {
		id = fmt.Sprintf("%s-%s", host, randomToken(3))
	}
	return &Agent{
		baseURL: strings.TrimRight(baseURL, "/"),
		id:      id,
		host:    host,
		weight:  weight,
		httpc:   &http.Client{Timeout: 10 * time.Second},
		clock:   newClockState(),
		phase:   agentPhaseInit,
	}, nil
}

func (a *Agent) postJSON(path string, req, resp interface{}) error {
	return a.postJSONCtx(context.Background(), path, req, resp)
}

func (a *Agent) postJSONCtx(ctx context.Context, path string, req, resp interface{}) error {
	body, err := json.Marshal(req)
	if err != nil {
		return err
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, a.baseURL+path, bytes.NewReader(body))
	if err != nil {
		return err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpResp, err := a.httpc.Do(httpReq)
	if err != nil {
		return err
	}
	defer httpResp.Body.Close()
	if err := json.NewDecoder(httpResp.Body).Decode(resp); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}
	return nil
}

// register connects (with retry) and applies any command the controller
// hands out immediately (late joiner).
func (a *Agent) register(ctx context.Context) error {
	backoff := 200 * time.Millisecond
	for {
		t1 := time.Now().UnixNano()
		req := MsgRegister{
			ProtocolVersion: clusterProtocolVersion,
			ResultVersion:   clusterResultVersion,
			PlowVersion:     version,
			AgentID:         a.id,
			SessionID:       a.session,
			Hostname:        a.host,
			Weight:          a.weight,
			Capabilities:    []string{"http"},
			T1NS:            t1,
		}
		var resp RegisterResponse
		err := a.postJSON("/plow/v1/register", req, &resp)
		t4 := time.Now().UnixNano()
		if err == nil && resp.ErrorCode != "" {
			// Protocol/version errors will never succeed - fail fast.
			return fmt.Errorf("registration rejected: %s %s", resp.ErrorCode, resp.Message)
		}
		if err == nil {
			a.clock.update(t1, resp.T2NS, resp.T3NS, t4)
			a.session = resp.SessionID
			if resp.HeartbeatIntervalNS > 0 {
				a.interval = time.Duration(resp.HeartbeatIntervalNS)
			}
			if resp.HeartbeatTimeoutNS > 0 {
				a.timeout = time.Duration(resp.HeartbeatTimeoutNS)
			}
			if resp.Command != nil {
				a.applyCommand(ctx, resp.Command)
			}
			return nil
		}
		fmt.Fprintf(os.Stderr, "@ agent %s register failed: %v, retrying in %s\n", a.id, err, backoff)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		if backoff < 3*time.Second {
			backoff *= 2
		}
	}
}

func (a *Agent) setPhase(p string) {
	a.mu.Lock()
	a.phase = p
	a.mu.Unlock()
}

func (a *Agent) getPhase() string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.phase
}

// Run drives the heartbeat loop until the controller marks the test
// done/aborted or ctx is cancelled.
func (a *Agent) Run(ctx context.Context) error {
	if err := a.register(ctx); err != nil {
		return err
	}
	kick := make(chan struct{}, 1)
	// A local requester finishing (quota done / stop / abort) must flush its
	// final stats immediately rather than waiting for the next interval.
	go func() {
		for {
			a.mu.Lock()
			c := a.collector
			a.mu.Unlock()
			if c != nil {
				select {
				case <-c.Done():
					a.setPhase(agentPhaseStopped)
					select {
					case kick <- struct{}{}:
					default:
					}
					return
				case <-ctx.Done():
					return
				}
			} else {
				select {
				case <-time.After(50 * time.Millisecond):
				case <-ctx.Done():
					return
				}
			}
		}
	}()

	var seq int64
	var failDeadline time.Time
	for {
		// Fire the first beat immediately, then on a jittered interval or
		// whenever a local event kicks us.
		timer := time.NewTimer(a.nextInterval())
		select {
		case <-ctx.Done():
			timer.Stop()
			if !a.crashOnStop {
				a.deregister()
			}
			return ctx.Err()
		case <-kick:
			timer.Stop()
		case <-timer.C:
		}

		seq++
		msg := a.buildHeartbeat(seq)
		var resp HeartbeatResponse
		err := a.postJSON("/plow/v1/heartbeat", msg, &resp)
		if err != nil {
			// Reuse the same seq on retry so the controller dedupes the
			// stats payload if the response was merely lost.
			seq--
			fmt.Fprintf(os.Stderr, "@ agent %s heartbeat failed: %v\n", a.id, err)
			if ctx.Err() != nil {
				if !a.crashOnStop {
					a.deregister()
				}
				return ctx.Err()
			}
			// Bound retries against a controller that is gone for good,
			// rather than looping forever.
			budget := a.timeout * 5
			if budget <= 0 {
				budget = 25 * time.Second
			}
			if failDeadline.IsZero() {
				failDeadline = time.Now().Add(budget)
			} else if time.Now().After(failDeadline) {
				return fmt.Errorf("controller unreachable for %s, giving up", budget)
			}
			time.Sleep(a.retryPause())
			continue
		}
		failDeadline = time.Time{}
		t4 := time.Now().UnixNano()
		a.clock.update(msg.T1NS, resp.T2NS, resp.T3NS, t4)

		if resp.ErrorCode == errCodeUnknownSession || resp.ErrorCode == errCodeStaleSession {
			fmt.Fprintf(os.Stderr, "@ agent %s session invalid (%s), re-registering\n", a.id, resp.ErrorCode)
			a.session = ""
			if err := a.register(ctx); err != nil {
				return err
			}
			continue
		}
		if resp.ErrorCode != "" {
			return fmt.Errorf("heartbeat rejected: %s %s", resp.ErrorCode, resp.Message)
		}
		if resp.Command != nil {
			a.applyCommand(ctx, resp.Command)
		}
		switch resp.TestPhase {
		case testPhaseDone:
			a.deregister()
			return nil
		case testPhaseAborted:
			a.deregister()
			a.mu.Lock()
			err := a.abortErr
			a.mu.Unlock()
			if err != nil {
				return err
			}
			return fmt.Errorf("test aborted by controller")
		}
	}
}

func (a *Agent) nextInterval() time.Duration {
	if a.interval <= 0 {
		return time.Second
	}
	// +/-15% jitter so nodes do not synchronize their heartbeats.
	f := 0.85 + rand.Float64()*0.3
	d := time.Duration(float64(a.interval) * f)
	return d
}

func (a *Agent) retryPause() time.Duration {
	d := a.interval / 2
	if d <= 0 {
		d = 200 * time.Millisecond
	}
	if d > time.Second {
		d = time.Second
	}
	return d
}

func (a *Agent) buildHeartbeat(seq int64) *MsgHeartbeat {
	t1 := time.Now().UnixNano()
	a.mu.Lock()
	phase := a.phase
	applied := a.applied
	c := a.collector
	a.mu.Unlock()
	msg := &MsgHeartbeat{
		ProtocolVersion: clusterProtocolVersion,
		ResultVersion:   clusterResultVersion,
		AgentID:         a.id,
		SessionID:       a.session,
		Seq:             seq,
		T1NS:            t1,
		Phase:           phase,
		AckedEpoch:      applied,
	}
	off, rtt, disp, _ := a.clock.snapshot()
	msg.ClockOffsetNS = off
	msg.ClockRTTNS = rtt
	msg.ClockDispersion = disp
	if c != nil {
		msg.Stats = c.snapshot(seq)
	}
	return msg
}

func (a *Agent) deregister() {
	if a.session == "" {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	req := MsgDeregister{AgentID: a.id, SessionID: a.session, Reason: "shutdown"}
	var resp DeregisterResponse
	_ = a.postJSONCtx(ctx, "/plow/v1/deregister", req, &resp)
}

// applyCommand executes prepare / set_rate / stop / abort instructions.
// Stale epochs are ignored, making duplicated delayed commands harmless.
func (a *Agent) applyCommand(ctx context.Context, cmd *Command) {
	if cmd.Epoch <= a.applied {
		return
	}
	switch cmd.Type {
	case cmdPrepare:
		if err := a.startLoad(ctx, cmd); err != nil {
			a.fail(fmt.Sprintf("prepare rejected: %v", err))
			return
		}
	case cmdSetRate:
		a.mu.Lock()
		rq := a.requester
		a.mu.Unlock()
		if rq != nil {
			rq.SetRate(limiterForRate(cmd.Rate))
			rq.SetQuota(cmd.Quota)
		}
	case cmdStop:
		a.mu.Lock()
		rq := a.requester
		a.mu.Unlock()
		if rq != nil {
			rq.Cancel()
		}
	case cmdAbort:
		a.mu.Lock()
		a.abortErr = fmt.Errorf("aborted by controller: %s", cmd.Reason)
		rq := a.requester
		a.mu.Unlock()
		if rq != nil {
			rq.Cancel()
		}
	}
	a.applied = cmd.Epoch
}

func (a *Agent) fail(msg string) {
	fmt.Fprintf(os.Stderr, "@ agent %s: %s\n", a.id, msg)
	a.setPhase(agentPhaseError)
}

func (a *Agent) startLoad(ctx context.Context, cmd *Command) error {
	cfg, err := cmd.VerifyConfig()
	if err != nil {
		return err
	}
	if cmd.Concurrency < 1 {
		return fmt.Errorf("assigned concurrency %d", cmd.Concurrency)
	}
	opt, err := clientOptFromConfig(cfg)
	if err != nil {
		return err
	}
	// Schedule in the controller time domain, convert to local clock using
	// the smoothed offset - this is what makes all nodes fire together.
	localStart := a.clock.localAlarm(time.Unix(0, cmd.StartAtNS))
	var localStop time.Time
	if cmd.StopAtNS > 0 {
		localStop = a.clock.localAlarm(time.Unix(0, cmd.StopAtNS))
	}
	rq, err := NewRequester(cmd.Concurrency, cmd.Quota, 0, limitValueForRate(cmd.Rate), io.Discard, opt, cfg.RampUp)
	if err != nil {
		return err
	}
	rq.SetSchedule(localStart, localStop)
	collector := newNodeCollector()

	a.mu.Lock()
	a.requester = rq
	a.collector = collector
	a.startAt = localStart
	a.phase = agentPhaseArmed
	a.mu.Unlock()

	go collector.Collect(rq.RecordChan())
	go rq.Run()
	// Flip to running at the scheduled local instant.
	go func() {
		wait := time.Until(localStart)
		if wait > 0 {
			t := time.NewTimer(wait)
			select {
			case <-t.C:
			case <-ctx.Done():
				t.Stop()
				return
			}
		}
		a.setPhase(agentPhaseRunning)
	}()
	fmt.Fprintf(os.Stderr, "@ agent %s armed: %d conns, %.1f rps, quota %d, start in %s\n",
		a.id, cmd.Concurrency, cmd.Rate, cmd.Quota, time.Until(localStart).Round(time.Millisecond))
	return nil
}

// limiterForRate builds the runtime limiter used for set_rate hot-swaps.
func limiterForRate(rps float64) *rate.Limiter {
	if rps <= 0 {
		return nil
	}
	return rate.NewLimiter(rate.Limit(rps), 1)
}

// limitValueForRate returns the rate value passed when constructing a new
// requester (the requester owns the limiter instance).
func limitValueForRate(rps float64) *rate.Limit {
	if rps <= 0 {
		return nil
	}
	l := rate.Limit(rps)
	return &l
}

func clientOptFromConfig(cfg *TestConfig) (*ClientOpt, error) {
	opt := &ClientOpt{
		url:          cfg.URL,
		method:       cfg.Method,
		headers:      cfg.Headers,
		contentType:  cfg.ContentType,
		host:         cfg.Host,
		insecure:     cfg.Insecure,
		maxConns:     cfg.Concurrency,
		doTimeout:    time.Duration(cfg.DoTimeoutNS),
		readTimeout:  time.Duration(cfg.ReadTimeoutNS),
		writeTimeout: time.Duration(cfg.WriteTimeoutNS),
		dialTimeout:  time.Duration(cfg.DialTimeoutNS),
		socks5Proxy:  cfg.Socks5Proxy,
		httpProxy:    cfg.HTTPProxy,
		unixSocket:   cfg.UnixSocket,
	}
	if cfg.BodyB64 != "" {
		b, err := base64.StdEncoding.DecodeString(cfg.BodyB64)
		if err != nil {
			return nil, fmt.Errorf("decode body: %w", err)
		}
		opt.bodyBytes = b
	}
	return opt, nil
}
