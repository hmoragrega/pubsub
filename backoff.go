package pubsub

import (
	"math"
	"math/rand"
	"time"
)

type BackoffStrategy interface {
	Delay(msg *Message) time.Duration
}

type BackoffStrategyFunc func(msg *Message) time.Duration

func (f BackoffStrategyFunc) Delay(msg *Message) time.Duration {
	return f(msg)
}

func LinearBackoff(delay time.Duration) BackoffStrategyFunc {
	return func(_ *Message) time.Duration {
		return delay
	}
}

type ExponentialBackoff struct {
	// Factor is the multiplying factor for each increment step, 3 by default.
	Factor float64
	// Max is the minimum delay, 1 minute by default.
	Min time.Duration
	// Max is the maximum delay, 24 hours by default.
	Max time.Duration
	// Jitter eases contention by randomizing backoff steps, 0 by default (disabled).
	// Must be a value [0.0, 1.0]
	Jitter float64
}

func (b *ExponentialBackoff) Delay(msg *Message) time.Duration {
	const maxDuration = 1<<63 - 1

	retry := float64(msg.ReceivedCount - 1)
	if retry < 0 {
		retry = 0
	}

	min := b.Min
	if min <= 0 {
		min = time.Minute
	}
	max := b.Max
	if max <= 0 {
		max = 24 * time.Hour
	}
	if min >= max {
		return max
	}
	factor := b.Factor
	if factor <= 0 {
		factor = 3
	}
	jitter := b.Jitter
	if jitter < 0 {
		jitter = 0
	}
	if jitter > 1 {
		jitter = 1
	}

	minFloat := float64(min)
	delayFloat := minFloat * math.Pow(factor, retry)

	if jitter > 0 {
		previous := minFloat * math.Pow(factor, retry-1)
		distance := delayFloat - previous
		jitterRange := jitter * distance
		jitterValue := ((randFloat64() * 2) -1) * jitterRange
		delayFloat = delayFloat + jitterValue
	}

	if delayFloat > maxDuration {
		return maxDuration
	}

	delay := time.Duration(delayFloat)

	if delay < min {
		return min
	}
	if delay > max {
		return max
	}

	return delay
}

var randFloat64 = rand.Float64