package main

import (
	"math"
	"sort"
)

// Global budget allocation across live nodes.
//
// The controller owns the global RPS / concurrency / request-count budgets the
// user requested and splits them over the current set of agents using a
// weighted largest-remainder method (Hare quota): weights default to 1 but let
// a beefier node (more CPU / bandwidth) take a larger share. Fractional parts
// are handed out one unit at a time in descending remainder order so the
// allocated totals always equal the requested totals exactly - no global RPS
// drift and no duplicated quotas.
//
// Every membership change (join / leave / failure) triggers a fresh
// allocation carried by a new-epoch set_rate command.

// allocWeights returns normalized weights; zero/negative weights become 1.
func allocWeights(weights []float64) []float64 {
	w := make([]float64, len(weights))
	for i, x := range weights {
		if x <= 0 {
			x = 1
		}
		w[i] = x
	}
	return w
}

// allocFloat64 distributes a fractional total (RPS) by weight.
// total <= 0 means "unlimited" and a zero allocation is returned per node;
// callers distinguish unlimited from zero via the config flag.
func allocFloat64(total float64, weights []float64) []float64 {
	n := len(weights)
	if n == 0 {
		return nil
	}
	out := make([]float64, n)
	if total <= 0 {
		for i := range out {
			out[i] = -1 // unlimited sentinel
		}
		return out
	}
	w := allocWeights(weights)
	sum := 0.0
	for _, x := range w {
		sum += x
	}
	floors := make([]float64, n)
	remainders := make([]float64, n)
	used := 0.0
	for i := range w {
		share := total * w[i] / sum
		f := math.Floor(share)
		floors[i] = f
		remainders[i] = share - f
		used += f
	}
	left := total - used
	order := remainderOrder(remainders)
	for _, idx := range order {
		if left <= 0 {
			break
		}
		// Hand out the largest fractional chunks (>= 1 each while possible) so
		// the sum is exact even for small totals.
		step := math.Min(1, left)
		floors[idx] += step
		left -= step
	}
	return floors
}

// allocInt64 distributes an integer total (concurrency or request quota).
// total < 0 means unlimited -> every node receives -1. Guaranteed exact sum.
func allocInt64(total int64, weights []float64) []int64 {
	n := len(weights)
	if n == 0 {
		return nil
	}
	out := make([]int64, n)
	if total < 0 {
		for i := range out {
			out[i] = -1
		}
		return out
	}
	w := allocWeights(weights)
	sum := 0.0
	for _, x := range w {
		sum += x
	}
	var floors int64
	frac := make([]float64, n)
	for i := range w {
		share := float64(total) * w[i] / sum
		f := int64(math.Floor(share))
		out[i] = f
		floors += f
		frac[i] = share - float64(f)
	}
	left := total - floors
	for _, idx := range remainderOrder(frac) {
		if left <= 0 {
			break
		}
		out[idx]++
		left--
	}
	return out
}

// remainderOrder returns indexes sorted by descending remainder, ties broken
// by index so allocations are deterministic.
func remainderOrder(remainders []float64) []int {
	order := make([]int, len(remainders))
	for i := range order {
		order[i] = i
	}
	sort.Slice(order, func(a, b int) bool {
		if remainders[order[a]] != remainders[order[b]] {
			return remainders[order[a]] > remainders[order[b]]
		}
		return order[a] < order[b]
	})
	return order
}

// redistribute computes new per-node budgets from the global totals and the
// currently live node set. For request-count tests the controller passes the
// *remaining* global quota rather than the original total, so failed/left
// nodes' unfinished share is taken over by survivors.
type nodeAlloc struct {
	rate        float64
	concurrency int64
	quota       int64
}

func redistribute(globalRate float64, globalConcurrency, remainingQuota int64, weights []float64) []nodeAlloc {
	rates := allocFloat64(globalRate, weights)
	concs := allocInt64(globalConcurrency, weights)
	quotas := allocInt64(remainingQuota, weights)
	out := make([]nodeAlloc, len(weights))
	for i := range out {
		out[i] = nodeAlloc{rate: rates[i], concurrency: concs[i], quota: quotas[i]}
	}
	return out
}
