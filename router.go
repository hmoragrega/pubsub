package pubsub

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/hashicorp/go-multierror"
)

var (
	ErrTopicAlreadyRegistered = errors.New("topic already registered")
	ErrRouterAlreadyRunning   = errors.New("router already running")
)

// Consumer consumes messages from a single subscription.
type consumer struct {
	subscriber Subscriber
	handler    MessageHandler
	topic      string
}

// Checkpoints are options hooks executed during the message live cycle,
// Returning an error will stop the subscription, and trigger the shutdown
// of the router.
type Checkpoint func(ctx context.Context, topic string, msg ReceivedMessage, err error) error

// Router groups consumers and runs them together.
type Router struct {
	// Message unmarshaler
	Unmarshaler Unmarshaler

	// DisableAutoAck disables automatic acknowledgement of the
	// messages. The handler will be responsible for it.
	DisableAutoAck bool

	// StopTimeout time to wait for all the consumer to stop in a
	// clean way. No timeout by default.
	StopTimeout time.Duration

	// Optional callback invoked when the consumer
	// reports an error.
	OnReceive Checkpoint

	// Optional callback invoked when the received message
	// cannot be unmarshaled into a message.
	OnUnmarshal Checkpoint

	// OnHandlerError callback invoked when the handler
	// returns an error.
	OnHandler Checkpoint

	// Optional callback invoked when the handled
	// message cannot be acknowledged
	OnAck Checkpoint

	consumers map[string]*consumer
	running   bool
	mx        sync.RWMutex
}

func (r *Router) RegisterHandler(topic string, subscriber Subscriber, handler MessageHandler) error {
	r.mx.Lock()
	defer r.mx.Unlock()

	if r.running {
		return ErrRouterAlreadyRunning
	}
	_, found := r.consumers[topic]
	if found {
		return fmt.Errorf("%w: %s", ErrTopicAlreadyRegistered, topic)
	}

	if r.consumers == nil {
		r.consumers = make(map[string]*consumer)
	}
	r.consumers[topic] = &consumer{
		topic:      topic,
		subscriber: subscriber,
		handler:    handler,
	}

	return nil
}

// Run starts all the consumer and keeps them running.
//
// Run is a blocking call, to stop it cancel the given context.
// If a consumer returns and error and "ContinueOnErrors" is
// "false" (default value), the router will stop all consumers
// and return the first error that triggered the shutdown.
//
// Calling run more than once will return an error.
// Registering new handlers after call running won't have
// any effect. It needs to be stopped and started again.
func (r *Router) Run(ctx context.Context) (err error) {
	if err = r.start(); err != nil {
		return
	}

	started, startErr := r.subscribe(r.consumers)
	defer func() {
		if stopErr := r.stop(started); stopErr != nil {
			// named return
			err = multierror.Append(err, stopErr)
		}
	}()

	if startErr != nil {
		err = multierror.Append(err, startErr)
		return
	}

	err = r.run(ctx, started)
	return
}

func (r *Router) subscribe(consumers map[string]*consumer) ([]*consumer, error) {
	var (
		started = make([]*consumer, 0, len(consumers))
		mx      sync.Mutex
	)

	var g multierror.Group
	for _, c := range consumers {
		c := c
		g.Go(func() error {
			subscribeErr := c.subscriber.Subscribe()
			if subscribeErr != nil {
				subscribeErr = fmt.Errorf("subscribe to topic %s failed: %w", c.topic, subscribeErr)
			}

			if subscribeErr == nil {
				mx.Lock()
				started = append(started, c)
				mx.Unlock()
			}

			return subscribeErr
		})
	}

	return started, g.Wait().ErrorOrNil()
}

func (r *Router) stop(consumers []*consumer) error {
	defer func() {
		r.mx.Lock()
		r.running = false
		r.mx.Unlock()
	}()

	ctx := context.Background()
	var cancel func()

	if t := r.stopTimeout(); t > 0 {
		ctx, cancel = context.WithTimeout(ctx, t)
		defer cancel()
	}

	var g multierror.Group
	for _, c := range consumers {
		c := c
		g.Go(func() error {
			stopErr := c.subscriber.Stop(ctx)
			if stopErr != nil {
				stopErr = fmt.Errorf("error stopping subscriber for topic %s: %w", c.topic, stopErr)
			}
			return stopErr
		})
	}

	return g.Wait().ErrorOrNil()
}

func (r *Router) run(ctx context.Context, consumers []*consumer) (err error) {
	var g multierror.Group
	for _, c := range consumers {
		c := c
		g.Go(func() error {
			consumerErr := r.consume(ctx, c)
			if consumerErr != nil {
				consumerErr = fmt.Errorf("error consuming from topic %s: %w", c.topic, consumerErr)
			}
			return consumerErr
		})
	}

	return g.Wait().ErrorOrNil()
}

func (r *Router) consume(ctx context.Context, c *consumer) error {
	for {
		msg, err := c.subscriber.Next(ctx)
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil
		}
		if err := r.check(ctx, r.OnReceive, c, msg, err); err != nil {
			return err
		}
		if err != nil {
			continue
		}

		message, err := r.Unmarshaler.Unmarshal(msg)
		if err := r.check(ctx, r.OnUnmarshal, c, msg, err); err != nil {
			return err
		}
		if err != nil {
			continue
		}

		err = c.handler.HandleMessage(ctx, message)
		if err := r.check(ctx, r.OnHandler, c, msg, err); err != nil {
			return err
		}
		if err != nil {
			continue
		}

		if r.DisableAutoAck {
			continue
		}
		err = msg.Ack(ctx)
		if err := r.check(ctx, r.OnAck, c, msg, err); err != nil {
			return err
		}
	}
}

func (r *Router) check(ctx context.Context, f Checkpoint, c *consumer, msg ReceivedMessage, err error) error {
	if f != nil {
		return f(ctx, c.topic, msg, err)
	}
	return nil
}

func (r *Router) stopTimeout() time.Duration {
	r.mx.RLock()
	defer r.mx.RUnlock()

	return r.StopTimeout
}

func (r *Router) start() error {
	r.mx.Lock()
	defer r.mx.Unlock()

	if r.running {
		return ErrRouterAlreadyRunning
	}

	r.running = true
	return nil
}
