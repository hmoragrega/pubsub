package pubsub

import (
	"math/rand"
	"testing"
	"time"
)

func TestLinearBackoff(t *testing.T) {
	delay := 5 * time.Minute

	testCases := []struct {
		name string
		msg  *Message
		want time.Duration
	}{
		{
			name: "exact delay on first attempt",
			msg:  &Message{ReceivedCount: 1},
			want: delay,
		}, {
			name: "exact delay on other attempts",
			msg:  &Message{ReceivedCount: 10},
			want: delay,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			b := LinearBackoff(delay)

			got := b.Delay(tc.msg)
			if got != tc.want {
				t.Errorf("unexpected delay, got %s, want %s", got, tc.want)
			}
		})
	}
}

func TestExponentialBackoff(t *testing.T) {
	t.Cleanup(func() {
		randFloat64 = rand.Float64
	})
	testCases := []struct {
		name                string
		initialReceiveCount int
		backoff             ExponentialBackoff
		randJitter          float64
		want                []string
	}{
		{
			name:                "default",
			initialReceiveCount: 1,
			want: []string{
				"1m",
				"3m",
				"9m",
				"27m",
				"1h21m",
				"4h3m",
				"12h9m",
				"24h",
				"24h",
			},
		}, {
			name: "default without receive count",
			want: []string{
				"1m",
				"1m",
				"3m",
				"9m",
				"27m",
				"1h21m",
				"4h3m",
				"12h9m",
				"24h",
				"24h",
			},
		}, {
			name: "higher factor",
			backoff: ExponentialBackoff{
				Factor: 6.66,
				Min:    5 * time.Second,
				Max:    24 * time.Hour,
			},
			initialReceiveCount: 1,
			want: []string{
				"5s",
				"33.3s",
				"3m41.778s",
				"24m37.04148s",
				"2h43m57.0962568s",
				"18h11m55.061070288s",
				"24h",
				"24h",
			},
		},
		{
			name:                "min jitter always",
			backoff:             ExponentialBackoff{Jitter: 0.1},
			randJitter:          0.0,
			initialReceiveCount: 1,
			want: []string{
				"1m0s", // min, no jitter
				"2m48s",
				"8m24s",
				"25m12s",
				"1h15m36s",
				"3h46m48s",
				"11h20m24s",
				"24h", // max, no jitter
				"24h",
			},
		}, {
			name:                "max jitter always",
			backoff:             ExponentialBackoff{Jitter: 0.1},
			randJitter:          1.0,
			initialReceiveCount: 1,
			want: []string{
				"1m4s", // min, no jitter
				"3m12s",
				"9m36s",
				"28m48s",
				"1h26m24s",
				"4h19m12s",
				"12h57m36s",
				"24h", // max, no jitter
				"24h",
			},
		}, {
			name:                "half jitter always - same as default",
			backoff:             ExponentialBackoff{Jitter: 0.1},
			randJitter:          0.5,
			initialReceiveCount: 1,
			want: []string{
				"1m",
				"3m",
				"9m",
				"27m",
				"1h21m",
				"4h3m",
				"12h9m",
				"24h",
				"24h",
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			randFloat64 = func() float64 {
				return tc.randJitter
			}

			for i, w := range tc.want {
				got := tc.backoff.Delay(&Message{ReceivedCount: tc.initialReceiveCount + i})

				want, _ := time.ParseDuration(w)
				if got != want {
					t.Errorf("[%d] unexpected delay, got %s, want %s", i, got, want)
				}
			}
		})
	}
}
