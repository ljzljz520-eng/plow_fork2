package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"sync"
	"time"
)

// Controller orchestrates the distributed run.
//
// Responsibilities:
//   - registration of agents and duplicate-node rejection (same hostname or
//     stale session can never be counted twice)
//   - clock-quality tracking and sizing of the synchronized-start lead time
//     so command delivery latency cannot make any node fire early
//   - splitting global RPS / concurrency / request quotas by node weight and
//     re-splitting on every dynamic join, graceful leave or detected failure
//   - heartbeat based failure detection
//   - idempotent, epoch-ordered schedule commands
//   - cumulative per-session stats merging into the shared SnapshotReport.

type controllerOptions struct {
	heartbeatInterval time.Duration
	heartbeatTimeout  time.Duration
	joinWait          time.Duration // window after the first registration
	minAgents         int
	minStartDelay     time.Duration
	maxClockError     time.Duration
	finishGrace       time.Duration
}

func defaultControllerOptions() controllerOptions {
	return controllerOptions{
		heartbeatInterval: time.Second,
		heartbeatTimeout:  5 * time.Second,
		joinWait:          3 * time.Second,
		minAgents:         1,
		minStartDelay:     2 * time.Second,
		maxClockError:     2 * time.Second,
		finishGrace:       3 * time.Second,
	}
}

type agentNode struct {
	id         string
	sessionID  string
	hostname   string
	plowVer    string
	weight     float64
	registered time.Time
	lastSeen   time.Time

	// clock quality reported by the agent
	clockKnown      bool
	clockOffsetNS   int64
	clockRTTNS      int64
	clockDispersion int64

	phase string // init / armed / running / stopped / error

	statsSeq int64 // highest merged stats sequence (dedupe / gap detect)

	alloc       nodeAlloc
	participant bool // ever received a prepare

	ackedEpoch int64
	pending    *Command // newest unacked command

	dead     bool
	gone     bool
	excluded bool // clock too uncertain at arm time
}

type Controller struct {
	cfg  *TestConfig
	opt  controllerOptions
	hash string

	mu       sync.Mutex
	phase    string
	nodes    map[string]*agentNode // agentID
	hosts    map[string]string     // hostname -> agentID of live node
	epoch    int64
	startAt  time.Time
	stopAt   time.Time
	armed    bool
	armTimer *time.Timer

	agg *clusterAggregator

	httpServer *http.Server
	listener   net.Listener

	stopCh chan struct{}
}

func newController(cfg *TestConfig, opt controllerOptions) (*Controller, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if opt.heartbeatInterval <= 0 {
		opt.heartbeatInterval = time.Second
	}
	if opt.heartbeatTimeout <= 0 {
		opt.heartbeatTimeout = 5 * time.Second
	}
	if opt.finishGrace <= 0 {
		opt.finishGrace = 3 * time.Second
	}
	c := &Controller{
		cfg:    cfg,
		opt:    opt,
		hash:   cfg.ConfigHash(),
		phase:  testPhaseIdle,
		nodes:  make(map[string]*agentNode),
		hosts:  make(map[string]string),
		agg:    newClusterAggregator(time.Now()),
		stopCh: make(chan struct{}),
	}
	return c, nil
}

func (c *Controller) Aggregator() *clusterAggregator { return c.agg }

func (c *Controller) Start(ctx context.Context, addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	c.listener = ln
	mux := http.NewServeMux()
	mux.HandleFunc("/plow/v1/register", c.handleRegister)
	mux.HandleFunc("/plow/v1/heartbeat", c.handleHeartbeat)
	mux.HandleFunc("/plow/v1/deregister", c.handleDeregister)
	c.httpServer = &http.Server{Handler: mux}
	go func() { _ = c.httpServer.Serve(ln) }()
	go c.failureDetector(ctx)
	go func() {
		select {
		case <-ctx.Done():
			c.abort("controller shutting down")
		case <-c.stopCh:
		}
	}()
	fmt.Fprintf(os.Stderr, "@ controller listening on http://%s\n", ln.Addr().String())
	return nil
}

func (c *Controller) Addr() string {
	if c.listener == nil {
		return ""
	}
	return c.listener.Addr().String()
}

func (c *Controller) Shutdown(ctx context.Context) {
	if c.httpServer != nil {
		_ = c.httpServer.Shutdown(ctx)
	}
}

func randomToken(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// ---- live node set helpers (caller holds c.mu) ----

func (c *Controller) liveNodesLocked() []*agentNode {
	out := make([]*agentNode, 0, len(c.nodes))
	for _, n := range c.nodes {
		if !n.dead && !n.gone {
			out = append(out, n)
		}
	}
	return out
}

func (c *Controller) participantNodesLocked() []*agentNode {
	out := make([]*agentNode, 0, len(c.nodes))
	for _, n := range c.nodes {
		if !n.dead && !n.gone && n.participant {
			out = append(out, n)
		}
	}
	return out
}

func (c *Controller) nextEpochLocked() int64 {
	c.epoch++
	return c.epoch
}

// ---- registration ----

func (c *Controller) handleRegister(w http.ResponseWriter, r *http.Request) {
	t2 := time.Now().UnixNano()
	var req MsgRegister
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeClusterJSON(w, http.StatusBadRequest, RegisterResponse{ErrorCode: errCodeBadRequest, Message: err.Error()})
		return
	}
	if err := versionsCompatible(req.ProtocolVersion, req.ResultVersion, req.PlowVersion); err != nil {
		writeClusterJSON(w, http.StatusBadRequest, RegisterResponse{ErrorCode: errCodeVersionMismatch, Message: err.Error()})
		return
	}

	c.mu.Lock()
	now := time.Now()

	// Duplicate physical-node guard: one live session per hostname.
	agentID := req.AgentID
	if agentID == "" {
		agentID = req.Hostname + "-" + randomToken(3)
	}
	if otherID, ok := c.hosts[req.Hostname]; ok && otherID != agentID {
		c.mu.Unlock()
		writeClusterJSON(w, http.StatusConflict, RegisterResponse{
			ErrorCode: errCodeDuplicateNode,
			Message:   fmt.Sprintf("hostname %q already registered as agent %s", req.Hostname, otherID),
		})
		return
	}

	n, exists := c.nodes[agentID]
	resuming := exists && req.SessionID != "" && req.SessionID == n.sessionID && !n.gone && !n.dead
	if exists && !resuming {
		// Same agent id reconnects with a new session: invalidate the
		// previous one so its stats freeze and the two sessions can never
		// overlap (which would double count the same node).
		c.agg.deactivate(n.sessionID)
		n.dead = true
		delete(c.hosts, n.hostname)
		n = nil
		exists = false
	}
	if !exists {
		n = &agentNode{
			id:         agentID,
			sessionID:  "s-" + randomToken(8),
			hostname:   req.Hostname,
			plowVer:    req.PlowVersion,
			weight:     req.Weight,
			registered: now,
			lastSeen:   now,
			phase:      agentPhaseInit,
		}
		c.nodes[agentID] = n
		c.hosts[req.Hostname] = agentID
	} else {
		n.lastSeen = now
	}

	resp := RegisterResponse{
		SessionID:           n.sessionID,
		HeartbeatIntervalNS: int64(c.opt.heartbeatInterval),
		HeartbeatTimeoutNS:  int64(c.opt.heartbeatTimeout),
		T1NS:                req.T1NS,
		T2NS:                t2,
		TestPhase:           c.phase,
	}

	lateJoin := c.phase == testPhaseRunning && !n.excluded && !n.participant
	if lateJoin {
		// Rebalance over the full set including the newcomer first, then hand
		// the newcomer a prepare carrying its final allocations, so it never
		// sees a tentative rate.
		n.participant = true
		n.phase = agentPhaseArmed
		c.rebalanceLocked("late join of agent " + agentID)
		if sr := n.pending; sr != nil {
			prep := c.prepareCommandLocked(c.nextEpochLocked(), c.startAt, c.stopAt)
			prep.Rate = sr.Rate
			prep.Concurrency = sr.Concurrency
			prep.Quota = sr.Quota
			n.pending = prep
			resp.Command = prep
		}
	}

	firstRegistration := c.phase == testPhaseIdle && !c.armed
	if firstRegistration {
		c.armed = true
		c.scheduleArm()
	}

	c.mu.Unlock()
	resp.T3NS = time.Now().UnixNano()
	writeClusterJSON(w, http.StatusOK, resp)

	if lateJoin {
		fmt.Fprintf(os.Stderr, "@ agent %s joined running test, schedule rebalanced\n", agentID)
	}
}

func (c *Controller) scheduleArm() {
	c.armTimer = time.AfterFunc(c.opt.joinWait, func() { c.arm() })
}

// arm computes the synchronized schedule and dispatches prepare commands.
func (c *Controller) arm() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.phase != testPhaseIdle {
		return
	}
	live := c.liveNodesLocked()
	var eligible, excluded []*agentNode
	for _, n := range live {
		if n.clockKnown && time.Duration(n.clockDispersion+n.clockRTTNS/2) > c.opt.maxClockError {
			n.excluded = true
			excluded = append(excluded, n)
		} else {
			eligible = append(eligible, n)
		}
	}
	if len(eligible) < c.opt.minAgents {
		c.abortLocked(fmt.Sprintf("only %d agent(s) synchronized within %s, need %d",
			len(eligible), c.opt.maxClockError, c.opt.minAgents))
		return
	}

	// Lead time must absorb: the agent poll interval until the command is
	// picked up, the round trip carrying it, and the residual clock error.
	var maxRTT, maxErr int64
	for _, n := range eligible {
		if n.clockRTTNS > maxRTT {
			maxRTT = n.clockRTTNS
		}
		if e := n.clockDispersion + n.clockRTTNS/2; e > maxErr {
			maxErr = e
		}
	}
	networkLead := 2*c.opt.heartbeatInterval + 2*time.Duration(maxRTT) + 2*time.Duration(maxErr) + 200*time.Millisecond
	lead := c.opt.minStartDelay
	if networkLead > lead {
		lead = networkLead
	}
	now := time.Now()
	c.startAt = now.Add(lead)
	if c.cfg.DurationNS > 0 {
		c.stopAt = c.startAt.Add(time.Duration(c.cfg.DurationNS))
	}
	c.agg.setSchedule(c.startAt, c.stopAt)
	go c.agg.run(c.stopCh)

	allocs := redistribute(c.cfg.Rate, int64(c.cfg.Concurrency), c.cfg.Requests, weightsOf(eligible))
	epoch := c.nextEpochLocked()
	cmd := c.prepareCommandLocked(epoch, c.startAt, c.stopAt)
	for i, n := range eligible {
		// Every participating node needs at least one connection even when
		// the global concurrency budget is smaller than the node count.
		if allocs[i].concurrency < 1 {
			allocs[i].concurrency = 1
		}
		n.alloc = allocs[i]
		n.participant = true
		n.pending = cloneCommandFor(cmd, n.alloc)
		n.phase = agentPhaseArmed
	}
	for _, n := range excluded {
		epoch = c.nextEpochLocked()
		n.pending = &Command{Epoch: epoch, Type: cmdAbort, Version: clusterResultVersion,
			Reason: "clock synchronization beyond configured max error"}
	}

	c.phase = testPhaseRunning
	fmt.Fprintf(os.Stderr, "@ arming %d agents (%d excluded), synchronized start in %s\n",
		len(eligible), len(excluded), lead.Round(time.Millisecond))

	if !c.stopAt.IsZero() {
		time.AfterFunc(time.Until(c.stopAt), func() { c.finish(cmdStop, "test duration reached") })
	}
}

func weightsOf(ns []*agentNode) []float64 {
	w := make([]float64, len(ns))
	for i, n := range ns {
		w[i] = n.weight
	}
	return w
}

func (c *Controller) prepareCommandLocked(epoch int64, startAt, stopAt time.Time) *Command {
	cmd := &Command{
		Epoch:     epoch,
		Type:      cmdPrepare,
		Version:   clusterResultVersion,
		Config:    c.cfg,
		Hash:      c.hash,
		StartAtNS: startAt.UnixNano(),
	}
	// time.Time{}.UnixNano() is a large negative number, not 0; only a
	// genuinely scheduled stop may appear on the wire.
	if !stopAt.IsZero() {
		cmd.StopAtNS = stopAt.UnixNano()
	}
	return cmd
}

func cloneCommandFor(proto *Command, a nodeAlloc) *Command {
	cmd := *proto
	rate := a.rate
	if rate < 0 {
		rate = 0 // 0 means unlimited on the wire
	}
	cmd.Rate = rate
	cmd.Concurrency = int(a.concurrency)
	cmd.Quota = a.quota
	return &cmd
}

// rebalanceLocked splits the *remaining* budgets over current participants
// and bumps the epoch so every node receives a set_rate command.
func (c *Controller) rebalanceLocked(reason string) {
	if c.phase != testPhaseRunning {
		return
	}
	parts := c.participantNodesLocked()
	if len(parts) == 0 {
		return
	}
	var remainingQuota int64 = -1
	if c.cfg.Requests > 0 {
		remainingQuota = c.cfg.Requests - c.agg.totalCount()
		if remainingQuota < 0 {
			remainingQuota = 0
		}
	}
	allocs := redistribute(c.cfg.Rate, int64(c.cfg.Concurrency), remainingQuota, weightsOf(parts))
	epoch := c.nextEpochLocked()
	for i, n := range parts {
		newRate := allocs[i].rate
		if newRate < 0 {
			newRate = 0
		}
		n.alloc = allocs[i]
		// set_rate only overrides rate/quota; concurrency is fixed for the
		// process lifetime of an agent.
		n.pending = &Command{
			Epoch:       epoch,
			Type:        cmdSetRate,
			Version:     clusterResultVersion,
			Rate:        newRate,
			Concurrency: int(allocs[i].concurrency),
			Quota:       allocs[i].quota,
			Reason:      reason,
		}
	}
	fmt.Fprintf(os.Stderr, "@ rebalanced %d live agents: %s\n", len(parts), reason)
}

// ---- heartbeat ----

func (c *Controller) handleHeartbeat(w http.ResponseWriter, r *http.Request) {
	t2 := time.Now().UnixNano()
	var req MsgHeartbeat
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeClusterJSON(w, http.StatusBadRequest, HeartbeatResponse{ErrorCode: errCodeBadRequest, Message: err.Error()})
		return
	}
	if req.ProtocolVersion != clusterProtocolVersion || req.ResultVersion != clusterResultVersion {
		writeClusterJSON(w, http.StatusBadRequest, HeartbeatResponse{ErrorCode: errCodeVersionMismatch})
		return
	}

	c.mu.Lock()
	n := c.nodes[req.AgentID]
	if n == nil || n.sessionID != req.SessionID || n.gone {
		c.mu.Unlock()
		writeClusterJSON(w, http.StatusNotFound, HeartbeatResponse{ErrorCode: errCodeUnknownSession, Message: "session expired, please re-register"})
		return
	}
	if n.dead {
		// A node previously failed by the detector is only readmitted via
		// fresh registration; accepting heartbeats here would double count.
		c.mu.Unlock()
		writeClusterJSON(w, http.StatusNotFound, HeartbeatResponse{ErrorCode: errCodeStaleSession, Message: "node marked failed, please re-register"})
		return
	}

	now := time.Now()
	n.lastSeen = now
	n.clockKnown = true
	n.clockOffsetNS = req.ClockOffsetNS
	n.clockRTTNS = req.ClockRTTNS
	n.clockDispersion = req.ClockDispersion

	duplicate := false
	if req.Stats != nil {
		if req.Stats.ResultVersion != clusterResultVersion {
			c.mu.Unlock()
			writeClusterJSON(w, http.StatusBadRequest, HeartbeatResponse{ErrorCode: errCodeResultMismatch})
			return
		}
		if req.Seq <= n.statsSeq {
			// Retried heartbeat: acknowledge but do not merge again, so
			// totals can never be double counted.
			duplicate = true
		} else if n.statsSeq != 0 && req.Seq > n.statsSeq+1 {
			c.mu.Unlock()
			writeClusterJSON(w, http.StatusConflict, HeartbeatResponse{ErrorCode: errCodeSeqGap, Message: "stats sequence gap"})
			return
		}
		if !duplicate {
			c.agg.applyStats(n.sessionID, n.id, req.Phase, req.Seq, req.Stats)
			n.statsSeq = req.Seq
		}
	}
	if req.Phase != "" {
		// Never move a stopped node backwards to running via a retried packet.
		if n.phase != agentPhaseStopped || req.Phase == agentPhaseStopped {
			n.phase = req.Phase
		}
	}
	if req.AckedEpoch >= n.ackedEpoch {
		n.ackedEpoch = req.AckedEpoch
	}
	if n.pending != nil && n.ackedEpoch >= n.pending.Epoch {
		n.pending = nil
	}

	c.checkCompletionLocked()

	resp := HeartbeatResponse{
		T2NS:      t2,
		TestPhase: c.phase,
		StartAtNS: c.startAt.UnixNano(),
		StopAtNS:  c.stopAt.UnixNano(),
	}
	if n.pending != nil {
		resp.Command = n.pending
	}
	if n.pending != nil && n.pending.Type == cmdPrepare {
		resp.StartAtNS = n.pending.StartAtNS
		resp.StopAtNS = n.pending.StopAtNS
	}
	c.mu.Unlock()
	resp.T3NS = time.Now().UnixNano()
	writeClusterJSON(w, http.StatusOK, resp)
}

// ---- graceful leave ----

func (c *Controller) handleDeregister(w http.ResponseWriter, r *http.Request) {
	var req MsgDeregister
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeClusterJSON(w, http.StatusBadRequest, DeregisterResponse{ErrorCode: errCodeBadRequest, Message: err.Error()})
		return
	}
	c.mu.Lock()
	n := c.nodes[req.AgentID]
	removed := false
	if n != nil && n.sessionID == req.SessionID {
		n.gone = true
		delete(c.hosts, n.hostname)
		c.agg.deactivate(n.sessionID)
		c.rebalanceLocked("agent " + n.id + " left")
		removed = true
		fmt.Fprintf(os.Stderr, "@ agent %s deregistered\n", n.id)
		// Last agent acknowledged the done phase: shut down immediately
		// instead of waiting out the full drain window.
		if c.phase == testPhaseDone && len(c.liveNodesLocked()) == 0 {
			c.teardownLocked()
		}
	}
	c.mu.Unlock()
	if !removed {
		writeClusterJSON(w, http.StatusNotFound, DeregisterResponse{ErrorCode: errCodeUnknownSession})
		return
	}
	writeClusterJSON(w, http.StatusOK, DeregisterResponse{})
}

// ---- failure detection ----

func (c *Controller) failureDetector(ctx context.Context) {
	interval := c.opt.heartbeatInterval / 2
	if interval <= 0 {
		interval = 500 * time.Millisecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-c.stopCh:
			return
		case <-ticker.C:
			c.sweepFailures()
		}
	}
}

func (c *Controller) sweepFailures() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.phase != testPhaseRunning && c.phase != testPhaseFinishing {
		return
	}
	now := time.Now()
	for _, n := range c.liveNodesLocked() {
		if now.Sub(n.lastSeen) > c.opt.heartbeatTimeout {
			n.dead = true
			delete(c.hosts, n.hostname)
			c.agg.deactivate(n.sessionID)
			fmt.Fprintf(os.Stderr, "@ agent %s missed heartbeats for %s, marked failed\n",
				n.id, c.opt.heartbeatTimeout)
			c.rebalanceLocked("agent " + n.id + " failed")
		}
	}
	c.checkCompletionLocked()
}

// ---- completion / termination ----

func (c *Controller) checkCompletionLocked() {
	if c.phase != testPhaseRunning {
		return
	}
	// Request-count mode finishes when the global quota has been completed
	// or every live participant reported stopped. Duration mode is driven by
	// the absolute stopAt timer only.
	if c.cfg.Requests > 0 && (c.agg.totalCount() >= c.cfg.Requests || c.allParticipantsStoppedLocked()) {
		c.finishLocked(cmdStop, "all work completed")
	}
}

func (c *Controller) allParticipantsStoppedLocked() bool {
	parts := c.participantNodesLocked()
	if len(parts) == 0 {
		return false
	}
	for _, n := range parts {
		if n.phase != agentPhaseStopped {
			return false
		}
	}
	return true
}

func (c *Controller) finish(cmdType, reason string) {
	c.mu.Lock()
	c.finishLocked(cmdType, reason)
	c.mu.Unlock()
}

func (c *Controller) finishLocked(cmdType, reason string) {
	if c.phase != testPhaseRunning {
		return
	}
	c.phase = testPhaseFinishing
	epoch := c.nextEpochLocked()
	for _, n := range c.participantNodesLocked() {
		n.pending = &Command{Epoch: epoch, Type: cmdType, Version: clusterResultVersion, Reason: reason}
	}
	fmt.Fprintf(os.Stderr, "@ finishing test: %s\n", reason)
	time.AfterFunc(c.opt.finishGrace, c.finalize)
}

func (c *Controller) abort(reason string) {
	c.mu.Lock()
	c.abortLocked(reason)
	c.mu.Unlock()
}

func (c *Controller) abortLocked(reason string) {
	if c.phase == testPhaseDone || c.phase == testPhaseAborted {
		return
	}
	// A finalize timer is already pending when finishing; it will observe
	// the aborted phase and tear down without scheduling a second close.
	finalizePending := c.phase == testPhaseFinishing
	c.phase = testPhaseAborted
	epoch := c.nextEpochLocked()
	for _, n := range c.nodes {
		if n.dead || n.gone {
			continue
		}
		n.pending = &Command{Epoch: epoch, Type: cmdAbort, Version: clusterResultVersion, Reason: reason}
	}
	fmt.Fprintf(os.Stderr, "@ test aborted: %s\n", reason)
	if !finalizePending {
		time.AfterFunc(c.opt.finishGrace, c.finalize)
	}
}

// finalize marks the test done but keeps the HTTP API serving for a short
// drain window, so every agent can observe the done phase and deregister
// instead of seeing the connection drop and retrying forever.
func (c *Controller) finalize() {
	c.mu.Lock()
	aborted := c.phase == testPhaseAborted
	if !aborted && c.phase != testPhaseRunning && c.phase != testPhaseFinishing {
		// Already finalized to done.
		c.mu.Unlock()
		return
	}
	if !aborted {
		c.phase = testPhaseDone
	}
	if len(c.liveNodesLocked()) == 0 {
		c.teardownLocked()
		c.mu.Unlock()
		return
	}
	drain := c.opt.finishGrace
	if min := 3 * c.opt.heartbeatInterval; min > drain {
		drain = min
	}
	if aborted {
		fmt.Fprintf(os.Stderr, "@ test aborted, draining agents for %s\n", drain)
	} else {
		fmt.Fprintf(os.Stderr, "@ test complete, draining agents for %s\n", drain)
	}
	c.mu.Unlock()
	time.AfterFunc(drain, c.teardown)
}

func (c *Controller) teardown() {
	c.mu.Lock()
	c.teardownLocked()
	c.mu.Unlock()
}

func (c *Controller) teardownLocked() {
	select {
	case <-c.stopCh:
		return
	default:
	}
	c.agg.Finish()
	close(c.stopCh)
}

func writeClusterJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
