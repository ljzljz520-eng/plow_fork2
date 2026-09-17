package main

import (
	"math"
	"testing"
)

func TestAllocInt64ExactSum(t *testing.T) {
	weights := []float64{1, 1, 1}
	for _, total := range []int64{1, 2, 3, 7, 100, 101} {
		got := allocInt64(total, weights)
		var sum int64
		for _, x := range got {
			if x < 0 {
				t.Fatalf("negative share %d for total %d", x, total)
			}
			sum += x
		}
		if sum != total {
			t.Fatalf("alloc total = %d, want %d (shares %v)", sum, total, got)
		}
	}
}

func TestAllocInt64Weights(t *testing.T) {
	// Weight 3:1 node should get roughly 75% and never less than the light node.
	got := allocInt64(1000, []float64{3, 1})
	if got[0] <= got[1] {
		t.Fatalf("weighted share wrong: %v", got)
	}
	if math.Abs(float64(got[0])-750) > 2 {
		t.Fatalf("heavy node share = %d, want ~750", got[0])
	}
}

func TestAllocInt64Unlimited(t *testing.T) {
	got := allocInt64(-1, []float64{1, 2})
	if got[0] != -1 || got[1] != -1 {
		t.Fatalf("unlimited shares = %v", got)
	}
}

func TestAllocFloat64ExactSum(t *testing.T) {
	got := allocFloat64(300, []float64{1, 1, 1})
	var sum float64
	for _, x := range got {
		if x <= 0 {
			t.Fatalf("non-positive share %v", got)
		}
		sum += x
	}
	if math.Abs(sum-300) > 1e-9 {
		t.Fatalf("rps total = %v, want 300", sum)
	}
}

func TestAllocFloat64Unlimited(t *testing.T) {
	got := allocFloat64(0, []float64{1, 1})
	if got[0] != -1 || got[1] != -1 {
		t.Fatalf("unexpected rps shares: %v", got)
	}
}

func TestRedistribute(t *testing.T) {
	allocs := redistribute(100, 10, -1, []float64{1, 1})
	if len(allocs) != 2 {
		t.Fatalf("got %d allocs", len(allocs))
	}
	var rps float64
	var conc int64
	for _, a := range allocs {
		rps += a.rate
		conc += a.concurrency
		if a.quota != -1 {
			t.Fatalf("quota = %d, want -1", a.quota)
		}
	}
	if math.Abs(rps-100) > 1e-9 || conc != 10 {
		t.Fatalf("redistribution wrong: rps %v conc %d", rps, conc)
	}
	// Remaining quota after a node failed with 30 done out of 100.
	allocs = redistribute(0, 4, 70, []float64{1})
	if allocs[0].rate != -1 || allocs[0].quota != 70 {
		t.Fatalf("survivor takeover wrong: %+v", allocs[0])
	}
}

func TestAllocZeroAndBadWeights(t *testing.T) {
	if got := allocInt64(0, []float64{1, 1}); got[0] != 0 || got[1] != 0 {
		t.Fatalf("zero total: %v", got)
	}
	// Zero/negative weights are treated as 1.
	got := allocInt64(10, []float64{0, -5})
	if got[0]+got[1] != 10 {
		t.Fatalf("bad weights alloc: %v", got)
	}
}
