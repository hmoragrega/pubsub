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
	ErrRouterAlreadyStopped   = errors.New("router already stopped")
)

type status uint8

const (
	started status = iota + 1
	stopped
)

type Acknowledgement uint8

const (
	NoOp Acknowledgement = iota
	Ack
	NAck
	ReSchedule
)

// Consumer consumes messages from a single subscription.
type consumer struct {
	subscriber Subscriber
	handler    Handler
	topic      string
	next       <-chan Next
	backoff    BackoffStrategy
}

// Checkpoint optional hooks executed during the message live cycle
//
// Returning an error will stop the subscription, and trigger the shutdown
// of the router.
type Checkpoint func(ctx context.Context, topic string, msg ReceivedMessage, err error) error

// DisableAutoAck is acknowledgement decider function that disables the router from
// auto-acknowledging messages.
func DisableAutoAck(ctx context.Context, topic string, msg ReceivedMessage, err error) Acknowledgement {
	return NoOp
}

// Router groups consumers and runs them together.
type Router struct {
	// Message unmarshaller. If none provided
	// NoOpUnmarshaller will be used.
	Unmarshaller Unmarshaller

	// AckDecider is an optional method that will decide if the message should
	// be acknowledged, negative acknowledged or do nothing; it receives the topic,
	// By default, the message will be acknowledged if there was no error handling it
	// and negatively acknowledged if there was.
	// To disable the automatic acknowledgements pass the DisableAutoAck function
	AckDecider func(ctx context.Context, topic string, msg ReceivedMessage, err error) Acknowledgement

	// Backoff calculates the time to wait when re-scheduling a message.
	// The default exponential back off will be used if not provided.
	// Note that consumers can override the back off strategy.
	Backoff BackoffStrategy

	// StopTimeout time to wait for all the consumer to stop in a
	// clean way. No timeout by default.
	StopTimeout time.Duration

	// MessageContext is an optional function that modify the context
	// that will be passed along during the message live-cycle:
	//  * in the message handler
	//  * in all the checkpoints.
	MessageContext func(parent context.Context, message ReceivedMessage) context.Context

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
	status    status
	mx        sync.RWMutex
}

type ConsumerOption func(*consumer)

func WithBackoff(strategy BackoffStrategy) func(*consumer) {
	return func(c *consumer) {
		c.backoff = strategy
	}
}

func (r *Router) Register(topic string, subscriber Subscriber, handler Handler, opts ...ConsumerOption) error {
	r.mx.Lock()
	defer r.mx.Unlock()

	if r.status == started {
		return ErrRouterAlreadyRunning
	}
	if r.status == stopped {
		return ErrRouterAlreadyStopped
	}

	_, found := r.consumers[topic]
	if found {
		return fmt.Errorf("%w: %s", ErrTopicAlreadyRegistered, topic)
	}

	if r.consumers == nil {
		r.consumers = make(map[string]*consumer)
	}
	c := &consumer{
		topic:      topic,
		subscriber: subscriber,
		handler:    handler,
	}
	for _, opt := range opts {
		opt(c)
	}

	r.consumers[topic] = c

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
			next, subscribeErr := c.subscriber.Subscribe()
			if subscribeErr != nil {
				subscribeErr = fmt.Errorf("subscribe to topic %s failed: %w", c.topic, subscribeErr)
			}

			if subscribeErr == nil {
				c.next = next
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
		r.status = stopped
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
	var next Next
	for {
		select {
		case <-ctx.Done():
			return nil
		case next = <-c.next:
		}

		rmsg, err := next.Message, next.Err
		ctx := r.messageContext(ctx, rmsg)

		if err := r.check(ctx, r.OnReceive, c, rmsg, err); err != nil {
			return err
		}
		if err != nil {
			continue
		}

		data, err := r.Unmarshaller.Unmarshal(c.topic, rmsg)
		if err := r.check(ctx, r.OnUnmarshal, c, rmsg, err); err != nil {
			return err
		}
		if err != nil {
			continue
		}

		msg := NewMessageFromReceived(rmsg, data)
		err = c.handler.HandleMessage(ctx, msg)
		if err := r.check(ctx, r.OnHandler, c, rmsg, err); err != nil {
			return err
		}

		switch r.ack(ctx, c, rmsg, err) {
		case Ack:
			err = msg.Ack(ctx)
		case NAck:
			err = msg.NAck(ctx)
		case ReSchedule:
			err = msg.ReSchedule(ctx, r.backoff(c, msg))
		case NoOp:
			continue
		}
		if err := r.check(ctx, r.OnAck, c, rmsg, err); err != nil {
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

	if r.status == started {
		return ErrRouterAlreadyRunning
	}
	if r.status == stopped {
		return ErrRouterAlreadyStopped
	}
	if r.Unmarshaller == nil {
		r.Unmarshaller = NoOpUnmarshaller()
	}
	if r.Backoff == nil {
		r.Backoff = &ExponentialBackoff{}
	}

	r.status = started
	return nil
}

func (r *Router) messageContext(ctx context.Context, msg ReceivedMessage) context.Context {
	if r.MessageContext == nil || msg == nil {
		return ctx
	}
	return r.MessageContext(ctx, msg)
}

func (r *Router) ack(ctx context.Context, c *consumer, msg ReceivedMessage, err error) Acknowledgement {
	if r.AckDecider == nil {
		if err != nil {
			return NAck
		}
		return Ack
	}

	return r.AckDecider(ctx, c.topic, msg, err)
}

func (r *Router) backoff(c *consumer, msg *Message) time.Duration {
	if c.backoff != nil {
		return c.backoff.Delay(msg)
	}

	return r.Backoff.Delay(msg)
}
