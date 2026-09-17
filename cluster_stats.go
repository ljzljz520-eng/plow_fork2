package main

import (
	"sort"
	"sync"
	"time"

	"github.com/beorn7/perks/histogram"
)

// Distributed statistics.
//
// Agents never ship individual samples: every heartbeat carries cumulative
// *moments* (count/sum/sumSq/min/max), status/error counters and the compact
// latency histogram. Cumulative counters are idempotent - the controller
// simply replaces each session's latest view, so a retried heartbeat can
// never double count anything. Per-session strictly increasing seq numbers
// additionally reject stale/duplicated deliveries. The mergeable quantities
// combine exactly across nodes:
//
//	count, sum, sumSq : additive
//	min, max          : min / max
//	codes, errors     : additive per key
//	bytes             : additive
//
// The adaptive perks histogram bins are not address-addressed, so the
// controller pools all node bins and re-runs the same min-gap compression to
// obtain the global 8-bin histogram; percentiles are then estimated from the
// merged CDF.

const clusterHistMaxBins = 8

type histBinDTO struct {
	Mean  float64 `json:"mean"`
	Count int     `json:"count"`
}

type statsMoments struct {
	Count int64   `json:"count"`
	Sum   float64 `json:"sum"`
	SumSq float64 `json:"sum_sq"`
	Min   float64 `json:"min"`
	Max   float64 `json:"max"`
}

func (m *statsMoments) update(v float64) {
	m.Count++
	m.Sum += v
	m.SumSq += v * v
	if m.Count == 1 || v < m.Min {
		m.Min = v
	}
	if v > m.Max {
		m.Max = v
	}
}

// mergeInto adds src into dst. The first sample initializes min/max.
func mergeMoments(dst *statsMoments, src statsMoments) {
	if src.Count == 0 {
		return
	}
	if dst.Count == 0 {
		dst.Min = src.Min
		dst.Max = src.Max
	} else {
		if src.Min < dst.Min {
			dst.Min = src.Min
		}
		if src.Max > dst.Max {
			dst.Max = src.Max
		}
	}
	dst.Count += src.Count
	dst.Sum += src.Sum
	dst.SumSq += src.SumSq
}

// NodeStats is the per-heartbeat stats payload. All totals are cumulative for
// the session; Window covers only samples since the previous heartbeat and is
// used by the controller for the real-time per-second charts.
type NodeStats struct {
	ResultVersion int   `json:"result_version"`
	Seq           int64 `json:"seq"`

	Latency statsMoments `json:"latency"`
	Window  statsMoments `json:"window"`

	Codes  map[int]int64    `json:"codes"`
	Errors map[string]int64 `json:"errors"`

	HistBins []histBinDTO `json:"hist_bins"`

	ReadBytes   int64 `json:"read_bytes"`
	WriteBytes  int64 `json:"write_bytes"`
	Concurrency int   `json:"concurrency"`
}

// ---- agent side collector ----

type nodeCollector struct {
	lock sync.Mutex

	lat    statsMoments
	win    statsMoments
	hist   *histogram.Histogram
	codes  map[int]int64
	errors map[string]int64

	readBytes   int64
	writeBytes  int64
	concurrency int

	doneChan chan struct{}
}

func newNodeCollector() *nodeCollector {
	return &nodeCollector{
		hist:     histogram.New(clusterHistMaxBins),
		codes:    make(map[int]int64, 4),
		errors:   make(map[string]int64, 4),
		doneChan: make(chan struct{}, 1),
	}
}

func (c *nodeCollector) Collect(records <-chan *ReportRecord) {
	for r := range records {
		c.lock.Lock()
		v := float64(r.cost)
		c.lat.update(v)
		c.win.update(v)
		c.hist.Insert(v)
		if r.code != 0 {
			c.codes[r.code]++
		}
		if r.error != "" {
			c.errors[r.error]++
		}
		c.readBytes = r.readBytes
		c.writeBytes = r.writeBytes
		c.concurrency = r.concurrencyCount
		c.lock.Unlock()
		recordPool.Put(r)
	}
	close(c.doneChan)
}

func (c *nodeCollector) Done() <-chan struct{} { return c.doneChan }

// snapshot returns the cumulative view plus the window accumulated since the
// previous snapshot and resets the window. Safe to call with no data.
func (c *nodeCollector) snapshot(seq int64) *NodeStats {
	c.lock.Lock()
	defer c.lock.Unlock()
	ns := &NodeStats{
		ResultVersion: clusterResultVersion,
		Seq:           seq,
		Latency:       c.lat,
		Window:        c.win,
		Codes:         make(map[int]int64, len(c.codes)),
		Errors:        make(map[string]int64, len(c.errors)),
		ReadBytes:     c.readBytes,
		WriteBytes:    c.writeBytes,
		Concurrency:   c.concurrency,
	}
	for k, v := range c.codes {
		ns.Codes[k] = v
	}
	for k, v := range c.errors {
		ns.Errors[k] = v
	}
	bins := c.hist.Bins()
	ns.HistBins = make([]histBinDTO, len(bins))
	for i, b := range bins {
		ns.HistBins[i] = histBinDTO{Mean: b.Mean(), Count: b.Count}
	}
	c.win = statsMoments{}
	return ns
}

// ---- controller side aggregation ----

type sessionAgg struct {
	agentID string
	active  bool
	phase   string
	lastSeq int64
	last    *NodeStats
}

type clusterAggregator struct {
	lock sync.Mutex

	sessions map[string]*sessionAgg
	startAt  time.Time
	stopAt   time.Time

	// per-second chart state, computed from the controller clock so the
	// merged RPS does not depend on agent wall clocks at all
	rpsStats        Stats
	lastTotalCount  int64
	lastTick        time.Time
	rpsWithinSec    float64
	windowWithinSec statsMoments
	noDataWithinSec bool

	doneOnce sync.Once
	doneChan chan struct{}
}

func newClusterAggregator(startAt time.Time) *clusterAggregator {
	return &clusterAggregator{
		sessions: make(map[string]*sessionAgg),
		startAt:  startAt,
		lastTick: startAt,
		doneChan: make(chan struct{}, 1),
	}
}

// applyStats replaces the session's cumulative view. Returns false when the
// payload is a duplicate (seq not newer) - it is acknowledged but not merged,
// guaranteeing at-most-once counting even with retried heartbeats.
func (a *clusterAggregator) applyStats(sessionID, agentID, phase string, seq int64, ns *NodeStats) bool {
	a.lock.Lock()
	defer a.lock.Unlock()
	s, ok := a.sessions[sessionID]
	if !ok {
		s = &sessionAgg{agentID: agentID, active: true, lastSeq: seq - 1}
		a.sessions[sessionID] = s
	}
	if seq <= s.lastSeq {
		return false
	}
	// A frozen session (node failed/left) can never be updated again; the
	// node must come back under a fresh session, preventing any overlap.
	if !s.active {
		return false
	}
	s.lastSeq = seq
	s.last = ns
	s.phase = phase
	// Fold the per-heartbeat window into the current controller-side bucket.
	mergeMoments(&a.windowWithinSec, ns.Window)
	return true
}

func (a *clusterAggregator) setSessionPhase(sessionID, phase string) {
	a.lock.Lock()
	defer a.lock.Unlock()
	if s, ok := a.sessions[sessionID]; ok {
		s.phase = phase
	}
}

// deactivate keeps a dead/gone session's final counters in the totals (its
// requests were real) but excludes it from live concurrency and further
// accounting. A reconnecting node gets a fresh session, so sessions never
// overlap and nothing is counted twice.
func (a *clusterAggregator) deactivate(sessionID string) {
	a.lock.Lock()
	defer a.lock.Unlock()
	if s, ok := a.sessions[sessionID]; ok {
		s.active = false
		s.phase = agentPhaseStopped
	}
}

func (a *clusterAggregator) totalCountLocked() int64 {
	var n int64
	for _, s := range a.sessions {
		if s.last != nil {
			n += s.last.Latency.Count
		}
	}
	return n
}

// totalCount returns the global completed sample count.
func (a *clusterAggregator) totalCount() int64 {
	a.lock.Lock()
	defer a.lock.Unlock()
	return a.totalCountLocked()
}

// run ticks once per controller second, deriving the global RPS series from
// cumulative counter deltas.
func (a *clusterAggregator) run(stop <-chan struct{}) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case now := <-ticker.C:
			a.lock.Lock()
			total := a.totalCountLocked()
			dc := total - a.lastTotalCount
			if dc > 0 {
				rps := float64(dc) / now.Sub(a.lastTick).Seconds()
				a.rpsStats.Update(rps)
				a.lastTotalCount = total
				a.lastTick = now
				a.rpsWithinSec = rps
				a.noDataWithinSec = false
			} else {
				a.rpsWithinSec = 0
				a.noDataWithinSec = true
			}
			a.windowWithinSec = statsMoments{}
			a.lock.Unlock()
		case <-stop:
			return
		case <-a.doneChan:
			return
		}
	}
}

// setSchedule pins the authoritative test window (controller clock) used for
// elapsed/RPS computation. Called when the controller finishes arming. The
// optional stop keeps the final average anchored to the test duration even if
// the controller keeps serving briefly to drain agents.
func (a *clusterAggregator) setSchedule(startAt, stopAt time.Time) {
	a.lock.Lock()
	a.startAt = startAt
	a.stopAt = stopAt
	a.lastTick = startAt
	a.lock.Unlock()
}

func (a *clusterAggregator) Finish() {
	a.doneOnce.Do(func() { close(a.doneChan) })
}

func (a *clusterAggregator) Done() <-chan struct{} { return a.doneChan }

// mergeHistBins pools node histograms and re-applies the perks min-gap
// compression, yielding the global <=clusterHistMaxBins bin histogram.
func mergeHistBins(sources ...[]histBinDTO) []histBinDTO {
	var bins []histBinDTO
	for _, src := range sources {
		bins = append(bins, src...)
	}
	if len(bins) == 0 {
		return nil
	}
	sort.SliceStable(bins, func(i, j int) bool { return bins[i].Mean < bins[j].Mean })
	for len(bins) > clusterHistMaxBins {
		idx := 0
		minGap := bins[1].Mean - bins[0].Mean
		for i := 1; i < len(bins)-1; i++ {
			if g := bins[i+1].Mean - bins[i].Mean; g < minGap {
				minGap = g
				idx = i
			}
		}
		a, b := bins[idx], bins[idx+1]
		cnt := a.Count + b.Count
		merged := histBinDTO{Count: cnt, Mean: (a.Mean*float64(a.Count) + b.Mean*float64(b.Count)) / float64(cnt)}
		bins = append(bins[:idx], append([]histBinDTO{merged}, bins[idx+2:]...)...)
	}
	return bins
}

// percentileFromBins estimates a quantile from the merged histogram CDF,
// linearly interpolating between adjacent bin means.
func percentileFromBins(bins []histBinDTO, q float64) float64 {
	if len(bins) == 0 {
		return 0
	}
	var total int64
	for _, b := range bins {
		total += int64(b.Count)
	}
	if total == 0 {
		return 0
	}
	if len(bins) == 1 {
		return bins[0].Mean
	}
	rank := q * float64(total-1)
	var before int64
	for i := range bins {
		after := before + int64(bins[i].Count)
		if rank < float64(after) || i == len(bins)-1 {
			frac := 0.0
			if bins[i].Count > 1 {
				frac = (rank - float64(before)) / float64(bins[i].Count-1)
			}
			if frac < 0 {
				frac = 0
			}
			if i == len(bins)-1 {
				return bins[i].Mean
			}
			return bins[i].Mean + frac*(bins[i+1].Mean-bins[i].Mean)
		}
		before = after
	}
	return bins[len(bins)-1].Mean
}

// Snapshot builds the same SnapshotReport shape the standalone Printer and
// Web charts consume, but sourced from every participating session.
func (a *clusterAggregator) Snapshot() *SnapshotReport {
	a.lock.Lock()
	defer a.lock.Unlock()

	var lat statsMoments
	codes := make(map[int]int64)
	errs := make(map[string]int64)
	var bins [][]histBinDTO
	var readBytes, writeBytes int64
	var concurrency int
	for _, s := range a.sessions {
		if s.last == nil {
			continue
		}
		mergeMoments(&lat, s.last.Latency)
		for k, v := range s.last.Codes {
			codes[k] += v
		}
		for k, v := range s.last.Errors {
			errs[k] += v
		}
		bins = append(bins, s.last.HistBins)
		readBytes += s.last.ReadBytes
		writeBytes += s.last.WriteBytes
		if s.active && s.phase == agentPhaseRunning {
			concurrency += s.last.Concurrency
		}
	}

	elapsed := time.Since(a.startAt)
	if !a.stopAt.IsZero() {
		if window := a.stopAt.Sub(a.startAt); elapsed > window {
			elapsed = window
		}
	}
	if elapsed <= 0 {
		elapsed = time.Millisecond
	}
	rs := &SnapshotReport{
		Elapsed: elapsed,
		Count:   lat.Count,
		Stats: &struct {
			Min    time.Duration
			Mean   time.Duration
			StdDev time.Duration
			Max    time.Duration
		}{},
		Codes: make(map[string]int64, 5),
	}
	if lat.Count > 0 {
		st := Stats{count: lat.Count, sum: lat.Sum, sumSq: lat.SumSq, min: lat.Min, max: lat.Max}
		rs.Stats.Min = time.Duration(lat.Min)
		rs.Stats.Mean = time.Duration(st.Mean())
		rs.Stats.StdDev = time.Duration(st.Stddev())
		rs.Stats.Max = time.Duration(lat.Max)
	}
	for k, v := range codes {
		if section, ok := httpStatusSectionLabelMap[k/100]; ok {
			rs.Codes[section] += v
		}
	}
	rs.Errors = errs

	elapseInSec := elapsed.Seconds()
	rs.RPS = float64(lat.Count) / elapseInSec
	rs.ReadThroughput = float64(readBytes) / 1024.0 / 1024.0 / elapseInSec
	rs.WriteThroughput = float64(writeBytes) / 1024.0 / 1024.0 / elapseInSec
	rs.concurrencyCount = concurrency

	if a.rpsStats.count > 0 {
		rs.RpsStats = &struct {
			Min    float64
			Mean   float64
			StdDev float64
			Max    float64
		}{a.rpsStats.min, a.rpsStats.Mean(), a.rpsStats.Stddev(), a.rpsStats.max}
	}

	merged := mergeHistBins(bins...)
	rs.Histograms = make([]*struct {
		Mean  time.Duration
		Count int
	}, len(merged))
	for i, b := range merged {
		rs.Histograms[i] = &struct {
			Mean  time.Duration
			Count int
		}{time.Duration(b.Mean), b.Count}
	}
	rs.Percentiles = make([]*struct {
		Percentile float64
		Latency    time.Duration
	}, len(quantiles))
	for i, p := range quantiles {
		rs.Percentiles[i] = &struct {
			Percentile float64
			Latency    time.Duration
		}{p, time.Duration(percentileFromBins(merged, p))}
	}
	return rs
}

// Charts produces the per-second real-time chart payload for the Web UI.
func (a *clusterAggregator) Charts() *ChartsReport {
	a.lock.Lock()
	defer a.lock.Unlock()
	if a.noDataWithinSec {
		return nil
	}
	cr := &ChartsReport{
		RPS:         a.rpsWithinSec,
		Latency:     Stats{count: a.windowWithinSec.Count, sum: a.windowWithinSec.Sum, sumSq: a.windowWithinSec.SumSq, min: a.windowWithinSec.Min, max: a.windowWithinSec.Max},
		CodeMap:     make(map[int]int64),
		Concurrency: 0,
	}
	for _, s := range a.sessions {
		if s.last == nil {
			continue
		}
		for k, v := range s.last.Codes {
			cr.CodeMap[k] += v
		}
		if s.active && s.phase == agentPhaseRunning {
			cr.Concurrency += s.last.Concurrency
		}
	}
	return cr
}
