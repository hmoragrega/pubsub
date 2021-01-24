package worker

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"
)

var (
	ErrMaxReached     = errors.New("maximum number of workers reached")
	ErrPoolClosed     = errors.New("worker pool is closed")
	ErrNotStarted     = errors.New("worker pool has not started")
	ErrInvalidMax     = errors.New("maximum workers must be equal or greater than minimum")
	ErrInvalidMin     = errors.New("minimum workers must be at least one")
	ErrInvalidInitial = errors.New("initial workers must match at least the minimum")
	ErrNotConfigured  = errors.New("worker pool not configured")
)

type PoolConfig struct {
	// Min indicates the maximum number of workers that can run concurrently.
	// When 0 is given the minimum is defaulted to 1.
	Min int

	// Max indicates the maximum number of workers that can run concurrently.
	// the default "0" indicates an infinite number of workers.
	Max int

	// Initial indicates the initial number of workers that should be running.
	// When 0 is given the minimum is used.
	Initial int

	// Interval sets a pause time to wait between jobs on each worker.
	Interval time.Duration

	// AutoStart indicates that the pool should be started on creation.
	AutoStart bool
}

func NewPool(job func()) (*Pool, error) {
	return NewPoolWithConfig(job, PoolConfig{})
}

func NewPoolWithConfig(job func(), cfg PoolConfig) (*Pool, error) {
	// defaults
	if cfg.Min == 0 {
		cfg.Min = 1
	}
	if cfg.Initial == 0 {
		cfg.Initial = cfg.Min
	}

	// validation
	if cfg.Min < 1 {
		return nil, fmt.Errorf("%w: min %d", ErrInvalidMin, cfg.Min)
	}
	if cfg.Max != 0 && cfg.Min > cfg.Max {
		return nil, fmt.Errorf("%w: max: %d, min %d", ErrInvalidMax, cfg.Max, cfg.Min)
	}
	if cfg.Initial < cfg.Min {
		return nil, fmt.Errorf("%w: initial: %d, min %d", ErrInvalidInitial, cfg.Initial, cfg.Min)
	}

	p := &Pool{
		job: job,
		cfg: &cfg,
	}
	if cfg.AutoStart {
		if err := p.Start(); err != nil {
			return nil, err
		}
	}
	return p, nil
}

func Must(p *Pool, err error) *Pool {
	if err != nil {
		panic(err)
	}
	return p
}

type Pool struct {
	job func()
	cfg *PoolConfig

	workers []*worker
	mx      sync.RWMutex

	started bool
	closed  bool
}

// Start launches the workers and keeps them running until the pool is closed.
// the method is idempotent and won't fail if it was call before.
//
// If the Start method after closing the pool it will return an error.
func (p *Pool) Start() error {
	p.mx.Lock()
	defer p.mx.Unlock()

	if p.cfg == nil {
		return ErrNotConfigured
	}
	if p.closed {
		return ErrPoolClosed
	}
	if p.started {
		return nil
	}
	p.started = true

	for i := 0; i < p.cfg.Initial; i++ {
		p.addWorker()
	}

	return nil
}

// More starts a new worker in the pool.
func (p *Pool) More() error {
	p.mx.Lock()
	defer p.mx.Unlock()

	if p.cfg == nil {
		return ErrNotConfigured
	}
	if p.closed {
		return ErrPoolClosed
	}
	if !p.started {
		return ErrNotStarted
	}
	if p.cfg.Max != 0 && len(p.workers) == p.cfg.Max {
		return ErrMaxReached
	}

	p.addWorker()
	return nil
}

// Less reduces the number of the workers in the pool.
//
// If the number of workers is already the minimum, the call
// won't have any effect and return nil.
//
// The current number of workers will decrement even if the
// context is cancelled or times out. The worker may still
// be executing the job but it has a pending signal to terminate.
func (p *Pool) Less(ctx context.Context) error {
	p.mx.Lock()
	defer p.mx.Unlock()

	if p.cfg == nil {
		return ErrNotConfigured
	}
	if p.closed {
		return ErrPoolClosed
	}
	current := len(p.workers)
	if current == p.cfg.Min {
		return nil
	}

	// pop the last workers. We can remove it since
	// we're going to call stop on the worker, and
	// whether stops before the context is cancelled
	// or not, is irrelevant, the unbuffered "quit"
	// signal will be sent and sooner or later the
	// worker will stop
	w := p.workers[current-1]
	p.workers = p.workers[:current-1]

	if err := w.stop(ctx); err != nil {
		return err
	}

	return nil
}

// Current returns the current number of workers.
func (p *Pool) Current() int {
	p.mx.RLock()
	defer p.mx.RUnlock()

	return len(p.workers)
}

// Close stops all the workers and closes the pool.
//
// Only the first call to Close will shutdown the pool,
// the next calls will be ignored.
//
// Once the pool is closed any other calls will trigger
// an error.
func (p *Pool) Close(ctx context.Context) error {
	p.mx.Lock()
	defer p.mx.Unlock()

	if p.cfg == nil {
		return ErrNotConfigured
	}
	if p.closed {
		return nil
	}

	// close the pool, the number of workers cannot increment.
	p.closed = true

	var g errgroup.Group
	for _, w := range p.workers {
		w := w
		g.Go(func() error {
			return w.stop(ctx)
		})
	}
	return g.Wait()
}

// CloseWIthTimeout displays the same behaviour as close, but
// instead of passing a context for cancellation we can pass
// a timeout value.
func (p *Pool) CloseWIthTimeout(timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	return p.Close(ctx)
}

// addWorker starts a new worker
//
// This method is not concurrently safe by it's own,
// it must be protected.
func (p *Pool) addWorker() {
	ctx, cancel := context.WithCancel(context.Background())
	w := &worker{
		done:   make(chan struct{}, 1),
		cancel: cancel,
	}
	w.start(ctx, p.job, p.cfg.Interval)

	p.workers = append(p.workers, w)
}

type worker struct {
	job      func()
	interval time.Duration
	cancel   func()
	done     chan struct{}
}

func (w *worker) start(ctx context.Context, job func(), interval time.Duration) {
	w.done = make(chan struct{}, 1)
	var (
		ticker *time.Ticker
		tick   <-chan time.Time
	)
	if interval > 0 {
		ticker = time.NewTicker(interval)
		tick = ticker.C
	} else {
		ch := make(chan time.Time)
		close(ch)
		tick = ch
	}
	go func() {
		defer func() {
			w.done <- struct{}{}
			if ticker != nil {
				ticker.Stop()
			}
		}()
		for {
			select {
			case <-ctx.Done():
				return
			case <-tick:
				job()
			}
		}
	}()
}

func (w *worker) stop(ctx context.Context) error {
	w.cancel()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-w.done:
	}

	return nil
}
