package worker

import (
	"context"
	"errors"
	"math"
	"math/rand"
	"sync"
	"testing"
	"time"
)

var dummyJob = func() {}

func TestPool_NewPool(t *testing.T) {
	testCases := []struct {
		name   string
		config PoolConfig
		want   error
	}{
		{
			name: "invalid min",
			config: PoolConfig{
				Min: -1,
			},
			want: ErrInvalidMin,
		},
		{
			name: "invalid max",
			config: PoolConfig{
				Min: 5,
				Max: 3,
			},
			want: ErrInvalidMax,
		},
		{
			name: "ok",
			config: PoolConfig{
				Initial: 7,
				Min:     5,
				Max:     10,
			},
			want: nil,
		},
		{
			name: "ok with defaults",
			want: nil,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, got := NewPoolWithConfig(dummyJob, tc.config)
			if !errors.Is(got, tc.want) {
				t.Fatalf("got error %+v, want: %+v", got, tc.want)
			}
		})
	}
}

func TestPool_Start(t *testing.T) {
	testCases := []struct {
		name   string
		config PoolConfig
		want   int
	}{
		{
			name: "initial value",
			config: PoolConfig{
				Initial: 7,
			},
			want: 7,
		},
		{
			name: "default ok",
			want: 1,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			p := Must(NewPoolWithConfig(dummyJob, tc.config))
			t.Cleanup(func() {
				_ = p.CloseWIthTimeout(time.Second)
			})

			if err := p.Start(); err != nil {
				t.Fatalf("unexpected error starting poole. got %+v", err)
			}
			if got := p.Current(); got != tc.want {
				t.Fatalf("unexpected initial workers. got %d, want: %d", got, tc.want)
			}
		})
	}
}

func TestPool_More(t *testing.T) {
	testCases := []struct {
		name        string
		pool        *Pool
		wantCurrent int
		wantErr     error
	}{
		{
			name: "max reached",
			pool: Must(NewPoolWithConfig(dummyJob, PoolConfig{
				Max: 1,
			})),
			wantCurrent: 1,
			wantErr:     ErrMaxReached,
		},
		{
			name: "ok",
			pool: Must(NewPoolWithConfig(dummyJob, PoolConfig{
				Max: 2,
			})),
			wantCurrent: 2,
			wantErr:     nil,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Cleanup(func() {
				_ = tc.pool.CloseWIthTimeout(time.Second)
			})
			if err := tc.pool.Start(); err != nil {
				t.Fatalf("unexpected error starting pool: %+v", err)
			}

			got := tc.pool.More()
			if !errors.Is(got, tc.wantErr) {
				t.Fatalf("got error %+v, want: %+v", got, tc.wantErr)
			}
			if current := tc.pool.Current(); current != tc.wantCurrent {
				t.Fatalf("unexpected number of workers: got %+v, want: %+v", current, tc.wantCurrent)
			}
		})
	}
}

func TestPool_Less(t *testing.T) {
	t.Run("reduce number of workers", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		p := Must(NewPool(dummyJob))
		t.Cleanup(func() {
			_ = p.Close(ctx)
		})

		if err := p.Start(); err != nil {
			t.Fatalf("unexpected error starting pool: %+v", err)
		}
		if err := p.More(); err != nil {
			t.Fatalf("unexpected error adding workers: %+v", err)
		}
		if current := p.Current(); current != 2 {
			t.Fatalf("unexpected number of workers: got %d, want 2", current)
		}

		if got := p.Less(ctx); got != nil {
			t.Fatalf("got error %+v, want nil", got)
		}
		if got := p.Current(); got != 1 {
			t.Fatalf("unexpected number of workers: got %d, want 1", got)
		}
	})

	t.Run("workers do not go below the minimum", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		p := Must(NewPool(dummyJob))
		t.Cleanup(func() {
			_ = p.Close(ctx)
		})

		if err := p.Start(); err != nil {
			t.Fatalf("unexpected error starting pool: %+v", err)
		}
		if current := p.Current(); current != 1 {
			t.Fatalf("unexpected number of workers: got %d, want 1", current)
		}

		if got := p.Less(ctx); got != nil {
			t.Fatalf("got error %+v, want nil", got)
		}
		if got := p.Current(); got != 1 {
			t.Fatalf("unexpected number of workers: got %d, want 1", got)
		}
	})

	t.Run("context terminated while reducing number of workers", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		p := Must(NewPool(func() {
			block := make(chan struct{})
			<-block
		}))
		t.Cleanup(func() {
			_ = p.Close(ctx)
		})

		if err := p.Start(); err != nil {
			t.Fatalf("unexpected error starting pool: %+v", err)
		}
		if err := p.More(); err != nil {
			t.Fatalf("unexpected error adding workers: %+v", err)
		}
		if current := p.Current(); current != 2 {
			t.Fatalf("unexpected number of workers: got %d, want 2", current)
		}

		if got := p.Less(ctx); !errors.Is(got, context.Canceled) {
			t.Fatalf("got error %+v, want %v", got, context.Canceled)
		}
		if got := p.Current(); got != 1 {
			t.Fatalf("unexpected number of workers: got %d, want 1", got)
		}
	})
}

func TestPool_Close(t *testing.T) {
	t.Run("close successfully a non started pool", func(t *testing.T) {
		p := Must(NewPool(dummyJob))
		got := p.Close(context.Background())
		if !errors.Is(got, nil) {
			t.Fatalf("unexpected error: %+v, want nil", got)
		}
	})

	t.Run("close successfully a started pool", func(t *testing.T) {
		p := Must(NewPool(dummyJob))
		if err := p.Start(); err != nil {
			t.Fatalf("unexpected error starting pool: %+v", err)
		}

		got := p.Close(context.Background())
		if !errors.Is(got, nil) {
			t.Fatalf("unexpected error closing pool: %+v, want nil", got)
		}
	})

	t.Run("close timeout error", func(t *testing.T) {
		running := make(chan struct{})
		p := Must(NewPool(func() {
			// signal that we are running the job
			running <- struct{}{}
			// block the job
			running <- struct{}{}
		}))
		if err := p.Start(); err != nil {
			t.Fatalf("unexpected error starting pool: %+v", err)
		}

		<-running
		got := p.CloseWIthTimeout(25 * time.Millisecond)
		if !errors.Is(got, context.DeadlineExceeded) {
			t.Fatalf("unexpected error closing pool: %+v, want %+v", got, context.DeadlineExceeded)
		}
	})

	t.Run("close cancelled error", func(t *testing.T) {
		p := Must(NewPool(func() {
			block := make(chan struct{})
			<-block
		}))
		if err := p.Start(); err != nil {
			t.Fatalf("unexpected error starting pool: %+v", err)
		}

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		got := p.Close(ctx)
		if !errors.Is(got, context.Canceled) {
			t.Fatalf("unexpected error closing pool: %+v, want %+v", got, context.Canceled)
		}
	})
}

func BenchmarkPool(b *testing.B) {
	tests := []struct {
		name     string
		count    int
		skipPool bool
	}{
		{
			name:  "10",
			count: 10,
		},
		{
			name:  "100",
			count: 100,
		},
		{
			name:  "1K",
			count: 1000,
		},
	}
	for _, tc := range tests {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
				p := Must(NewPoolWithConfig(dummyJob, PoolConfig{Interval: 200 * time.Millisecond}))
				if err := p.Start(); err != nil {
					b.Fatal("cannot start pool", err)
				}
				for j := 0; j < tc.count; j++ {
					if err := p.More(); err != nil {
						b.Fatal("cannot add worker", err)
					}
					if math.Mod(float64(j+1), 10) == 0 {
						if err := p.Less(ctx); err != nil {
							b.Fatal("cannot remove worker", err)
						}
					}
				}
				if err := p.Close(ctx); err != nil {
					b.Fatal("cannot close pool", err)
				}
			}
		})
	}
}

func TestConcurrencySafety(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	p := Must(NewPoolWithConfig(dummyJob, PoolConfig{Interval: 200 * time.Millisecond}))
	if err := p.Start(); err != nil {
		t.Fatal("cannot start pool", err)
	}

	var wg sync.WaitGroup
	wg.Add(2)

	offset := 100
	go func() {
		for i := 0; i < rand.Intn(1000)+offset; i++ {
			if err := p.More(); err != nil {
				t.Fatal("cannot add worker", err)
			}
		}
		wg.Done()
	}()
	go func() {
		for i := 0; i < rand.Intn(1000); i++ {
			if err := p.Less(ctx); err != nil {
				t.Fatal("cannot remove worker", err)
			}
		}
		wg.Done()
	}()

	wg.Wait()
	if err := p.Close(ctx); err != nil {
		t.Fatal("cannot close pool", err)
	}
}
