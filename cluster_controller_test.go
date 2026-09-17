package main

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/valyala/fasthttp"
)

func startTestTarget(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		_ = fasthttp.Serve(ln, func(ctx *fasthttp.RequestCtx) {
			ctx.SetStatusCode(fasthttp.StatusOK)
		})
	}()
	t.Cleanup(func() { _ = ln.Close() })
	return "http://" + ln.Addr().String() + "/"
}

func testControllerOptions() controllerOptions {
	return controllerOptions{
		heartbeatInterval: 40 * time.Millisecond,
		heartbeatTimeout:  250 * time.Millisecond,
		joinWait:          200 * time.Millisecond,
		minAgents:         1,
		minStartDelay:     300 * time.Millisecond,
		maxClockError:     5 * time.Second,
		finishGrace:       300 * time.Millisecond,
	}
}

func newTestAgent(t *testing.T, suffix string) *Agent {
	t.Helper()
	a, err := newAgent("http://unused", "agent-"+suffix, "host-"+suffix, 1)
	if err != nil {
		t.Fatal(err)
	}
	a.baseURL = "" // set by caller through controller addr after creation
	return a
}

// runAgentAgainst runs an agent against a controller address.
func runAgentAgainst(ctx context.Context, a *Agent, addr string) error {
	a.baseURL = "http://" + addr
	return a.Run(ctx)
}

func waitDone(t *testing.T, ctrl *Controller, timeout time.Duration) {
	t.Helper()
	select {
	case <-ctrl.Aggregator().Done():
	case <-time.After(timeout):
		t.Fatal("controller did not finish in time")
	}
}

func sessionCounts(ctrl *Controller) map[string]int64 {
	agg := ctrl.Aggregator()
	agg.lock.Lock()
	defer agg.lock.Unlock()
	out := make(map[string]int64, len(agg.sessions))
	for _, s := range agg.sessions {
		if s.last != nil {
			out[s.agentID] = s.last.Latency.Count
		} else {
			out[s.agentID] = 0
		}
	}
	return out
}

// Full lifecycle: two agents armed with a synchronized start, one crashes
// (heartbeat failure detection), a third joins late, duration-based stop and
// merged totals.
func TestClusterDurationRunJoinLeaveFailure(t *testing.T) {
	target := startTestTarget(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := &TestConfig{
		ProtocolVersion: clusterProtocolVersion,
		URL:             target,
		Method:          "GET",
		Concurrency:     6,
		Rate:            1000,
		Requests:        -1,
		DurationNS:      (1500 * time.Millisecond).Nanoseconds(),
		RampUp:          6,
	}
	ctrl, err := newController(cfg, testControllerOptions())
	if err != nil {
		t.Fatal(err)
	}
	if err := ctrl.Start(ctx, "127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	defer func() {
		sctx, scancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer scancel()
		ctrl.Shutdown(sctx)
	}()
	addr := ctrl.Addr()

	a1 := newTestAgent(t, "a1")
	a2 := newTestAgent(t, "a2")
	ctx1, cancel1 := context.WithCancel(ctx)
	ctx2, kill2 := context.WithCancel(ctx)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); _ = runAgentAgainst(ctx1, a1, addr) }()
	go func() { defer wg.Done(); _ = runAgentAgainst(ctx2, a2, addr) }()
	a2.crashOnStop = true

	// Crash a2 (no deregister) shortly after the run starts.
	time.AfterFunc(750*time.Millisecond, kill2)

	// Late joiner after the synchronized start.
	a3 := newTestAgent(t, "a3")
	time.AfterFunc(900*time.Millisecond, func() {
		go func() { _ = runAgentAgainst(ctx, a3, addr) }()
	})

	waitDone(t, ctrl, 8*time.Second)
	cancel1()
	final := ctrl.Aggregator().Snapshot()

	if final.Count < 500 {
		t.Fatalf("merged count too low: %d", final.Count)
	}
	if got := final.Codes["2xx"]; got != final.Count {
		t.Fatalf("2xx = %d, want %d", got, final.Count)
	}
	if len(final.Errors) != 0 {
		t.Fatalf("unexpected errors: %v", final.Errors)
	}
	counts := sessionCounts(ctrl)
	if counts["agent-a1"] == 0 {
		t.Fatalf("agent-a1 produced no traffic: %v", counts)
	}
	if counts["agent-a3"] == 0 {
		t.Fatalf("late joiner agent-a3 produced no traffic: %v", counts)
	}
	if counts["agent-a2"] == 0 {
		t.Fatalf("crashed agent-a2 contribution was lost: %v", counts)
	}
	// The RPS cap across the cluster must not be grossly exceeded.
	elapsed := final.Elapsed.Seconds()
	if rps := float64(final.Count) / elapsed; rps > 2500 {
		t.Fatalf("cluster RPS %v far above 1000 budget (elapsed %v)", rps, elapsed)
	}
	// Give a1/a3 a moment to observe done and return.
	doneCh := make(chan struct{})
	go func() { wg.Wait(); close(doneCh) }()
	select {
	case <-doneCh:
	case <-time.After(2 * time.Second):
	}
}

// Request-count mode must complete exactly the global quota, split across
// nodes with no double counting and no loss.
func TestClusterQuotaExact(t *testing.T) {
	target := startTestTarget(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := &TestConfig{
		ProtocolVersion: clusterProtocolVersion,
		URL:             target,
		Method:          "GET",
		Concurrency:     4,
		Requests:        100,
		RampUp:          4,
	}
	ctrl, err := newController(cfg, testControllerOptions())
	if err != nil {
		t.Fatal(err)
	}
	if err := ctrl.Start(ctx, "127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	defer func() {
		sctx, scancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer scancel()
		ctrl.Shutdown(sctx)
	}()

	var wg sync.WaitGroup
	for _, id := range []string{"q1", "q2"} {
		a := newTestAgent(t, id)
		wg.Add(1)
		go func(a *Agent) { defer wg.Done(); _ = runAgentAgainst(ctx, a, ctrl.Addr()) }(a)
	}
	waitDone(t, ctrl, 6*time.Second)
	final := ctrl.Aggregator().Snapshot()
	if final.Count != 100 {
		t.Fatalf("count = %d, want exactly 100", final.Count)
	}
	wg.Wait()
}

// Registration guards: protocol mismatch and duplicate physical node.
func TestClusterRegistrationGuards(t *testing.T) {
	target := startTestTarget(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := &TestConfig{
		ProtocolVersion: clusterProtocolVersion,
		URL:             target,
		Method:          "GET",
		Concurrency:     2,
		DurationNS:      (time.Second).Nanoseconds(),
	}
	opt := testControllerOptions()
	opt.joinWait = 2 * time.Second
	ctrl, err := newController(cfg, opt)
	if err != nil {
		t.Fatal(err)
	}
	if err := ctrl.Start(ctx, "127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	defer func() {
		sctx, scancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer scancel()
		ctrl.Shutdown(sctx)
	}()

	a := newTestAgent(t, "guard")
	a.baseURL = "http://" + ctrl.Addr()
	var resp RegisterResponse
	bad := MsgRegister{
		ProtocolVersion: 42,
		ResultVersion:   clusterResultVersion,
		PlowVersion:     version,
		AgentID:         "bad",
		Hostname:        "h-bad",
		T1NS:            time.Now().UnixNano(),
	}
	if err := a.postJSONCtx(ctx, "/plow/v1/register", bad, &resp); err != nil {
		t.Fatal(err)
	}
	if resp.ErrorCode != errCodeVersionMismatch {
		t.Fatalf("error = %q, want %q", resp.ErrorCode, errCodeVersionMismatch)
	}

	// Two different agent ids sharing a hostname collide.
	first := MsgRegister{ProtocolVersion: clusterProtocolVersion, ResultVersion: clusterResultVersion,
		PlowVersion: version, AgentID: "dup1", Hostname: "dup-host", Weight: 1, T1NS: time.Now().UnixNano()}
	var r1, r2 RegisterResponse
	if err := a.postJSONCtx(ctx, "/plow/v1/register", first, &r1); err != nil {
		t.Fatal(err)
	}
	second := first
	second.AgentID = "dup2"
	if err := a.postJSONCtx(ctx, "/plow/v1/register", second, &r2); err != nil {
		t.Fatal(err)
	}
	if r2.ErrorCode != errCodeDuplicateNode {
		t.Fatalf("error = %q, want %q", r2.ErrorCode, errCodeDuplicateNode)
	}
}

// A heartbeat from an unknown session must be rejected so the agent
// re-registers instead of being counted under a stale identity.
func TestClusterUnknownSessionRejected(t *testing.T) {
	target := startTestTarget(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cfg := &TestConfig{ProtocolVersion: clusterProtocolVersion, URL: target, Method: "GET", Concurrency: 1}
	ctrl, err := newController(cfg, testControllerOptions())
	if err != nil {
		t.Fatal(err)
	}
	if err := ctrl.Start(ctx, "127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	defer func() {
		sctx, scancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer scancel()
		ctrl.Shutdown(sctx)
	}()

	a := newTestAgent(t, "unknown")
	a.baseURL = "http://" + ctrl.Addr()
	var resp HeartbeatResponse
	hb := MsgHeartbeat{
		ProtocolVersion: clusterProtocolVersion,
		ResultVersion:   clusterResultVersion,
		AgentID:         "ghost",
		SessionID:       "s-ghost",
		Seq:             1,
		T1NS:            time.Now().UnixNano(),
		Phase:           agentPhaseInit,
	}
	if err := a.postJSONCtx(ctx, "/plow/v1/heartbeat", hb, &resp); err != nil {
		t.Fatal(err)
	}
	if resp.ErrorCode != errCodeUnknownSession {
		t.Fatalf("error = %q, want %q", resp.ErrorCode, errCodeUnknownSession)
	}
}

func TestClusterSmokeNoAgentsAbortOnCancel(t *testing.T) {
	target := startTestTarget(t)
	ctx, cancel := context.WithCancel(context.Background())
	cfg := &TestConfig{ProtocolVersion: clusterProtocolVersion, URL: target, Method: "GET", Concurrency: 1,
		DurationNS: (time.Second).Nanoseconds()}
	ctrl, err := newController(cfg, testControllerOptions())
	if err != nil {
		t.Fatal(err)
	}
	if err := ctrl.Start(ctx, "127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	defer func() {
		sctx, scancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer scancel()
		ctrl.Shutdown(sctx)
	}()
	cancel()
	waitDone(t, ctrl, 3*time.Second)
	ctrl.mu.Lock()
	phase := ctrl.phase
	ctrl.mu.Unlock()
	if phase != testPhaseAborted {
		t.Fatalf("phase = %q, want %q", phase, testPhaseAborted)
	}
}
