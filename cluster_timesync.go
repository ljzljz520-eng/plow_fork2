package main

import (
	"math"
	"sync"
	"time"
)

// Clock synchronization (NTP-lite).
//
// Each request/response round trip produces the classic four timestamps:
//
//	t1  agent sends request
//	t2  controller receives it
//	t3  controller sends response
//	t4  agent receives it
//
// Assuming forward and backward path delays are symmetric:
//
//	offset  = ((t2 - t1) + (t3 - t4)) / 2     controller - agent clock delta
//	rtt     = (t4 - t1) - (t3 - t2)           network round trip time
//
// The true offset lies within [offset - rtt/2, offset + rtt/2]. Samples are
// smoothed with an EWMA and a dispersion (error bound) is tracked, so the
// controller can size the lead time of a scheduled start and refuse to arm
// nodes whose clocks are too uncertain. This removes the dependency on
// externally synchronized wall clocks (NTP/chrony) between load nodes.

const (
	clockEWMAAlpha     = 0.25
	clockDispersionPhi = 0.125
)

type clockSample struct {
	offsetNS int64
	rttNS    int64
}

// ntpSample computes one raw (offset, rtt) sample from four timestamps.
func ntpSample(t1, t2, t3, t4 int64) clockSample {
	off := float64((t2-t1)+(t3-t4)) / 2.0
	rtt := float64((t4 - t1) - (t3 - t2))
	if rtt < 0 {
		// Scheduler jitter can produce tiny negatives; clamp to zero.
		rtt = 0
	}
	return clockSample{offsetNS: int64(math.Round(off)), rttNS: int64(math.Round(rtt))}
}

// clockState is the agent-side smoothed view of the controller clock.
type clockState struct {
	mu sync.Mutex

	synced     bool
	offsetNS   int64
	rttNS      int64
	dispersion int64
	samples    int
}

func newClockState() *clockState { return &clockState{} }

func (c *clockState) update(t1, t2, t3, t4 int64) clockSample {
	s := ntpSample(t1, t2, t3, t4)
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.synced {
		c.synced = true
		c.offsetNS = s.offsetNS
		c.rttNS = s.rttNS
		c.dispersion = s.rttNS / 2
	} else {
		drift := math.Abs(float64(s.offsetNS - c.offsetNS))
		c.offsetNS = int64(math.Round(clockEWMAAlpha*float64(s.offsetNS) + (1-clockEWMAAlpha)*float64(c.offsetNS)))
		c.rttNS = int64(math.Round(clockEWMAAlpha*float64(s.rttNS) + (1-clockEWMAAlpha)*float64(c.rttNS)))
		// Dispersion = smoothed (drift + half RTT uncertainty).
		d := clockDispersionPhi*(drift+float64(s.rttNS)/2) + (1-clockDispersionPhi)*float64(c.dispersion)
		c.dispersion = int64(math.Round(d))
	}
	c.samples++
	return s
}

func (c *clockState) snapshot() (offset, rtt, dispersion int64, synced bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.offsetNS, c.rttNS, c.dispersion, c.synced
}

// controllerTime converts an agent wall-clock instant to controller domain.
func (c *clockState) controllerTime(t time.Time) time.Time {
	c.mu.Lock()
	off := c.offsetNS
	c.mu.Unlock()
	return t.Add(time.Duration(off))
}

// localAlarm converts a controller-domain target time to the agent wall-clock
// instant at which the event must fire. All nodes therefore fire the same
// controller-instant despite different local clocks.
func (c *clockState) localAlarm(controllerAt time.Time) time.Time {
	c.mu.Lock()
	off := c.offsetNS
	c.mu.Unlock()
	return controllerAt.Add(-time.Duration(off))
}

// withinErrorBound reports whether the current synchronization is precise
// enough to honor a synchronized start with maxError tolerance.
func (c *clockState) withinErrorBound(maxError time.Duration) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.synced {
		return false
	}
	// Total firing error is bounded by clock dispersion plus half the latest
	// RTT (the residual asymmetric path delay).
	return time.Duration(c.dispersion+c.rttNS/2) <= maxError
}
