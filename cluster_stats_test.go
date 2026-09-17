package main

import (
	"testing"
	"time"
)

func TestMergeMoments(t *testing.T) {
	var dst statsMoments
	mergeMoments(&dst, statsMoments{Count: 3, Sum: 30, SumSq: 300, Min: 5, Max: 15})
	mergeMoments(&dst, statsMoments{Count: 2, Sum: 40, SumSq: 800, Min: 10, Max: 30})
	if dst.Count != 5 || dst.Sum != 70 || dst.SumSq != 1100 || dst.Min != 5 || dst.Max != 30 {
		t.Fatalf("merge wrong: %+v", dst)
	}
	st := Stats{count: dst.Count, sum: dst.Sum, sumSq: dst.SumSq, min: dst.Min, max: dst.Max}
	if st.Mean() != 14 {
		t.Fatalf("mean = %v, want 14", st.Mean())
	}
}

func TestAggregatorDedupeAndMerge(t *testing.T) {
	agg := newClusterAggregator(time.Now())
	ns := func(seq int64, count int64, sum float64) *NodeStats {
		return &NodeStats{
			ResultVersion: clusterResultVersion,
			Seq:           seq,
			Latency:       statsMoments{Count: count, Sum: sum, SumSq: sum * sum, Min: 1, Max: 10},
			Codes:         map[int]int64{200: count},
			Errors:        map[string]int64{},
			Window:        statsMoments{Count: count, Sum: sum, SumSq: sum * sum, Min: 1, Max: 10},
		}
	}
	// Session A: seq 1 then duplicate seq 1 (retried response loss).
	if !agg.applyStats("sA", "a", agentPhaseRunning, 1, ns(1, 10, 100)) {
		t.Fatal("first stats must apply")
	}
	if agg.applyStats("sA", "a", agentPhaseRunning, 1, ns(1, 10, 100)) {
		t.Fatal("duplicate seq must not apply again")
	}
	if !agg.applyStats("sA", "a", agentPhaseRunning, 2, ns(2, 20, 200)) {
		t.Fatal("seq 2 must apply")
	}
	// Session B joins, then fails; its final totals stay in the aggregate.
	agg.applyStats("sB", "b", agentPhaseRunning, 1, ns(1, 5, 50))
	agg.deactivate("sB")
	// Late packets for the frozen session must never alter totals again.
	if agg.applyStats("sB", "b", agentPhaseStopped, 2, ns(2, 999, 999)) {
		t.Fatal("stats for a deactivated session must be rejected")
	}

	if got := agg.totalCount(); got != 25 {
		t.Fatalf("total count = %d, want 25", got)
	}
	snap := agg.Snapshot()
	if snap.Count != 25 {
		t.Fatalf("snapshot count = %d, want 25", snap.Count)
	}
	if snap.Codes["2xx"] != 25 {
		t.Fatalf("codes = %v", snap.Codes)
	}
	if snap.concurrencyCount != 0 {
		t.Fatalf("inactive node B must not count toward concurrency, got %d", snap.concurrencyCount)
	}
}

func TestAggregatorNewSessionDoesNotDoubleCountReconnect(t *testing.T) {
	agg := newClusterAggregator(time.Now())
	ns := &NodeStats{ResultVersion: clusterResultVersion, Seq: 1,
		Latency: statsMoments{Count: 10, Sum: 10, SumSq: 10, Min: 1, Max: 1},
		Codes:   map[int]int64{200: 10}}
	agg.applyStats("sOld", "a", agentPhaseStopped, 1, ns)
	agg.deactivate("sOld")
	// Same node reconnects with a fresh session starting its own counters.
	ns2 := &NodeStats{ResultVersion: clusterResultVersion, Seq: 1,
		Latency: statsMoments{Count: 3, Sum: 3, SumSq: 3, Min: 1, Max: 1},
		Codes:   map[int]int64{200: 3}}
	agg.applyStats("sNew", "a", agentPhaseRunning, 1, ns2)
	if got := agg.totalCount(); got != 13 {
		t.Fatalf("count = %d, want frozen(10) + new(3) = 13", got)
	}
}

func TestMergeHistBinsCompressesAndOrders(t *testing.T) {
	a := []histBinDTO{{Mean: 1, Count: 10}, {Mean: 10, Count: 5}}
	b := []histBinDTO{{Mean: 2, Count: 8}, {Mean: 11, Count: 2}}
	merged := mergeHistBins(a, b)
	if len(merged) > clusterHistMaxBins {
		t.Fatalf("too many bins: %d", len(merged))
	}
	var total int
	for i := range merged {
		total += merged[i].Count
		if i > 0 && merged[i].Mean < merged[i-1].Mean {
			t.Fatal("bins must be sorted")
		}
	}
	if total != 25 {
		t.Fatalf("merged count = %d, want 25", total)
	}
}

func TestPercentileFromBinsMonotonic(t *testing.T) {
	bins := []histBinDTO{{Mean: 1, Count: 25}, {Mean: 5, Count: 50}, {Mean: 20, Count: 25}}
	prev := 0.0
	for _, q := range []float64{0.5, 0.9, 0.95, 0.99} {
		v := percentileFromBins(bins, q)
		if v < prev {
			t.Fatalf("percentile decreased at %v: %f < %f", q, v, prev)
		}
		prev = v
	}
	if v := percentileFromBins(bins, 0.5); v < 1 || v > 20 {
		t.Fatalf("p50 out of range: %f", v)
	}
}

func TestNodeCollectorWindowReset(t *testing.T) {
	c := newNodeCollector()
	for i := 0; i < 5; i++ {
		c.win.update(float64(time.Millisecond))
		c.lat.update(float64(time.Millisecond))
	}
	s1 := c.snapshot(1)
	if s1.Window.Count != 5 || s1.Latency.Count != 5 {
		t.Fatalf("first snapshot wrong: %+v", s1)
	}
	c.win.update(float64(time.Millisecond))
	c.lat.update(float64(time.Millisecond))
	s2 := c.snapshot(2)
	if s2.Window.Count != 1 || s2.Latency.Count != 6 {
		t.Fatalf("second snapshot wrong: window=%d total=%d", s2.Window.Count, s2.Latency.Count)
	}
}
