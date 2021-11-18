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
	ErrConsumerAlreadyRegistered = errors.New("consumer already registered")
	ErrRouterAlreadyRunning      = errors.New("router already running")
	ErrRouterAlreadyStopped      = errors.New("router already stopped")
)

type status uint8

const (
	started status = iota + 1
	stopped
)

type AckDecider func(ctx context.Context, topic string, msg ReceivedMessage, err error) Acknowledgement

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
	name       string
	next       <-chan Next
	backoff    BackoffStrategy
	ackDecider AckDecider
}

// Checkpoint optional hooks executed during the message live cycle
//
// Returning an error will stop the subscription, and trigger the shutdown
// of the router.
type Checkpoint func(ctx context.Context, consumerName string, msg ReceivedMessage, err error) error

// OnProcess optional hook called after processing a received message in a consumer.
type OnProcess func(ctx context.Context, consumerName string, elapsed time.Duration, msg ReceivedMessage, err error)

func AutoAck(_ context.Context, _ string, _ ReceivedMessage, err error) Acknowledgement {
	if err != nil {
		return NAck
	}
	return Ack
}

// DisableAutoAck is acknowledgement decider function that disables the router from
// auto-acknowledging messages.
func DisableAutoAck(_ context.Context, _ string, _ ReceivedMessage, _ error) Acknowledgement {
	return NoOp
}

// ReScheduleOnError is acknowledgement decider function re-schedules on errors.
func ReScheduleOnError(_ context.Context, _ string, _ ReceivedMessage, err error) Acknowledgement {
	if err != nil {
		return ReSchedule
	}
	return Ack
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
	AckDecider AckDecider

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

	// Optional callback invoked after fully processing a message
	// passing the elapsed time and the error, if any.
	OnProcess OnProcess

	consumers map[string]*consumer
	status    status
	mx        sync.RWMutex
}

type ConsumerOption func(*consumer)

func WithAckDecider(ackDecider AckDecider) func(*consumer) {
	return func(c *consumer) {
		c.ackDecider = ackDecider
	}
}

func WithBackoff(strategy BackoffStrategy) func(*consumer) {
	return func(c *consumer) {
		c.backoff = strategy
	}
}

func (r *Router) Register(name string, subscriber Subscriber, handler Handler, opts ...ConsumerOption) error {
	r.mx.Lock()
	defer r.mx.Unlock()

	if r.status == started {
		return ErrRouterAlreadyRunning
	}
	if r.status == stopped {
		return ErrRouterAlreadyStopped
	}

	_, found := r.consumers[name]
	if found {
		return fmt.Errorf("%w: %s", ErrConsumerAlreadyRegistered, name)
	}

	if r.consumers == nil {
		r.consumers = make(map[string]*consumer)
	}
	c := &consumer{
		name:       name,
		subscriber: subscriber,
		handler:    handler,
	}
	for _, opt := range opts {
		opt(c)
	}

	r.consumers[name] = c

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
				subscribeErr = fmt.Errorf("subscribe to name %s failed: %w", c.name, subscribeErr)
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
				stopErr = fmt.Errorf("error stopping subscriber for consumer %s: %w", c.name, stopErr)
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
				consumerErr = fmt.Errorf("error consuming from consumer %s: %w", c.name, consumerErr)
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

		if err := next.Err; err != nil {
			if err := r.check(ctx, r.OnReceive, c, nil, err); err != nil {
				return err
			}
			continue
		}

		if err := r.processMessage(ctx, c, next.Message); err != nil {
			return err
		}
	}
}

func (r *Router) processMessage(ctx context.Context, c *consumer, m ReceivedMessage) (err error) {
	ctx = r.messageContext(ctx, m)

	start := time.Now()
	defer func() {
		if p := r.OnProcess; p != nil {
			p(ctx, c.name, time.Since(start), m, err)
		}
	}()

	if err := r.check(ctx, r.OnReceive, c, m, err); err != nil {
		return err
	}
	if err != nil {
		return nil
	}

	data, err := r.Unmarshaller.Unmarshal(c.name, m)
	if err := r.check(ctx, r.OnUnmarshal, c, m, err); err != nil {
		return err
	}
	if err != nil {
		return nil
	}

	msg := NewMessageFromReceived(m, data)
	err = c.handler.HandleMessage(ctx, msg)
	if err := r.check(ctx, r.OnHandler, c, m, err); err != nil {
		return err
	}

	switch r.ack(ctx, c, m, err) {
	case Ack:
		err = msg.Ack(ctx)
	case NAck:
		err = msg.NAck(ctx)
	case ReSchedule:
		err = msg.ReSchedule(ctx, r.backoff(c, msg))
	case NoOp:
		return nil
	}

	return r.check(ctx, r.OnAck, c, m, err)
}

func (r *Router) check(ctx context.Context, f Checkpoint, c *consumer, msg ReceivedMessage, err error) error {
	if f != nil {
		return f(ctx, c.name, msg, err)
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
	if r.AckDecider == nil {
		r.AckDecider = AutoAck
	}
	if r.Backoff == nil {
		r.Backoff = &ExponentialBackoff{}
	}

	r.status = started
	return nil
}

func (r *Router) messageContext(ctx context.Context, msg ReceivedMessage) context.Context {
	if r.MessageContext == nil {
		return ctx
	}
	return r.MessageContext(ctx, msg)
}

func (r *Router) ack(ctx context.Context, c *consumer, msg ReceivedMessage, err error) Acknowledgement {
	if c.ackDecider != nil {
		return c.ackDecider(ctx, c.name, msg, err)
	}

	return r.AckDecider(ctx, c.name, msg, err)
}

func (r *Router) backoff(c *consumer, msg *Message) time.Duration {
	if c.backoff != nil {
		return c.backoff.Delay(msg)
	}

	return r.Backoff.Delay(msg)
}

func WrapOnProcess(hooks ...OnProcess) OnProcess {
	return func(ctx context.Context, consumerName string, elapsed time.Duration, msg ReceivedMessage, err error) {
		for _, hook := range hooks {
			hook(ctx, consumerName, elapsed, msg, err)
		}
	}
}
