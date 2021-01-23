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
	ErrMaxReached = errors.New("maximum number of workers reached")
	ErrPoolClosed = errors.New("worker pool is closed")
	ErrNotStarted = errors.New("worker pool has not started")
	ErrInvalidMax = errors.New("maximum workers must be equal or greater than minimum")
	ErrInvalidMin = errors.New("minimum workers must be at least one")
	ErrInvalidJob = errors.New("missing workers job")
)

type Pool struct {
	// Min indicates the maximum number of workers that can run concurrently.
	// When 0 is given the minimum is defaulted to 1.
	// Changes to this value after the pool start won't take effect.
	Min int

	// Max indicates the maximum number of workers that can run concurrently.
	// the default "0" indicates an infinite number of workers.
	// Changes to this value after the pool start won't take effect.
	Max int

	// Job is the function that the workers will be running non-stop.
	// Changing this job after the pool has started will affect all
	// current and future workers.
	Job func()

	min     int
	max     int
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

	if p.closed {
		return ErrPoolClosed
	}
	if p.started {
		return nil
	}
	p.started = true

	if err := p.validate(); err != nil {
		return err
	}

	p.min = p.Min
	p.max = p.Max
	for i := 0; i < p.min; i++ {
		p.addWorker()
	}

	return nil
}

func (p *Pool) validate() error {
	if p.Job == nil {
		return ErrInvalidJob
	}
	if p.Min == 0 {
		p.Min = 1
	}
	if p.Min < 1 {
		return fmt.Errorf("%w: min %d", ErrInvalidMin, p.Min)
	}
	if p.Max != 0 && p.Min > p.Max {
		return fmt.Errorf("%w: max: %d, min %d", ErrInvalidMax, p.Max, p.Min)
	}
	return nil
}

// More starts a new worker in the pool.
func (p *Pool) More() error {
	p.mx.Lock()
	defer p.mx.Unlock()

	if p.closed {
		return ErrPoolClosed
	}
	if !p.started {
		return ErrNotStarted
	}

	if p.max != 0 && len(p.workers) == p.max {
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

	if p.closed {
		return ErrPoolClosed
	}

	current := len(p.workers)
	if current == p.min {
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

	// check if its already closed.
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
	w := &worker{job: p.Job}
	p.workers = append(p.workers, w)
	w.start()
}

type worker struct {
	job  func()
	quit chan struct{}
	done chan struct{}
}

func (w *worker) start() {
	w.quit = make(chan struct{}, 2)
	w.done = make(chan struct{})

	go func() {
		defer func() {
			w.done <- struct{}{}
		}()
		for {
			select {
			case <-w.quit:
				return
			default:
				w.job()
			}
		}
	}()
}

func (w *worker) stop(ctx context.Context) error {
	w.quit <- struct{}{}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-w.done:
	}

	return nil
}
