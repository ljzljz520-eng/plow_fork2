package main

import (
	"testing"
	"time"
)

func TestNTPSampleSymmetricPath(t *testing.T) {
	// Agent clock 50ms behind controller, symmetric 10ms each way.
	const off = int64(50 * time.Millisecond)
	const path = int64(10 * time.Millisecond)
	t1 := int64(1_000_000_000)
	t2 := t1 + path + off
	t3 := t2 + int64(time.Millisecond)
	t4 := t3 + path - off
	s := ntpSample(t1, t2, t3, t4)
	if s.offsetNS != off {
		t.Fatalf("offset = %d, want %d", s.offsetNS, off)
	}
	if want := int64(20 * time.Millisecond); s.rttNS != want {
		t.Fatalf("rtt = %d, want %d", s.rttNS, want)
	}
}

func TestClockStateSmoothesAndConverts(t *testing.T) {
	c := newClockState()
	if _, _, _, synced := c.snapshot(); synced {
		t.Fatal("clock must start unsynced")
	}
	base := int64(10_000_000_000)
	for i := 0; i < 10; i++ {
		t1 := base + int64(i)*int64(100*time.Millisecond)
		// offset 100ms, jittery RTT between 4 and 12ms.
		path := int64(4*time.Millisecond) + int64((i%3))*int64(4*time.Millisecond)
		t2 := t1 + path + int64(100*time.Millisecond)
		t3 := t2 + int64(time.Millisecond)
		t4 := t3 + path - int64(100*time.Millisecond)
		c.update(t1, t2, t3, t4)
	}
	off, rtt, disp, synced := c.snapshot()
	if !synced {
		t.Fatal("expected synced")
	}
	if got := time.Duration(off); got < 90*time.Millisecond || got > 110*time.Millisecond {
		t.Fatalf("smoothed offset %s out of range", got)
	}
	if rtt <= 0 || disp < 0 {
		t.Fatalf("bad rtt/dispersion: %d %d", rtt, disp)
	}
	// localAlarm: controller target maps back to local time.
	target := time.Unix(0, base+int64(time.Second))
	alarm := c.localAlarm(target)
	if d := target.Sub(alarm); d < 90*time.Millisecond || d > 110*time.Millisecond {
		t.Fatalf("local alarm delta = %s, want ~100ms", d)
	}
	if !c.withinErrorBound(5 * time.Second) {
		t.Fatal("expected clock within generous error bound")
	}
}

func TestNTPSampleNegativeRTTClamped(t *testing.T) {
	s := ntpSample(1000, 1001, 1005, 1002)
	if s.rttNS < 0 {
		t.Fatalf("rtt must be clamped to >= 0, got %d", s.rttNS)
	}
}
