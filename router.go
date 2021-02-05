package pubsub

import (
	"context"
	"fmt"
	"sync"
)

// Router groups a consumers and runs them together.
type Router struct {
	// Message unmarshaler
	Unmarshaler Unmarshaler
	// Optional callback invoked when the consumer
	// reports an error.
	OnReceive func(topic string, message ReceivedMessage, err error) error
	// Optional callback invoked when the consumer
	// the message has no handler associated
	OnUnregistered func(topic string, message ReceivedMessage) error
	// Optional callback invoked when the received message
	// cannot be unmarshaled into a message.
	OnUnmarshal func(topic string, message ReceivedMessage, err error) error
	// OnHandlerError callback invoked when the handler
	// returns an error.
	OnHandler func(topic string, message ReceivedMessage, err error) error
	// Optional callback invoked when the handled
	// message cannot be acknowledged
	OnAck func(topic string, message ReceivedMessage, err error) error
	// Disables automatic acknowledgement of the messages
	// The handler will be responsible for it.
	DisableAutoAck bool
	// ContinueOnErrors when true the router won't stop if a consumer
	// returns an error.
	ContinueOnErrors bool

	consumers   map[string]*Consumer
	consumersMX sync.RWMutex
}

// Run keeps all consumers running
//
// Terminate the context for a clean exit
//
// If a consumer returns and error and
// ContinueOnErrors is false (default),
// the router will stop and return the
// first consumer error.
func (r *Router) Run(ctx context.Context) error {
	// TODO improvement: start async
	for _, c := range r.consumers {
		if err := c.Subscribe(); err != nil {
			// TODO stop started on failure.
			return err
		}
	}

	ctx, cancel := context.WithCancel(ctx)

	var (
		err error
		wg  sync.WaitGroup
	)

	for _, c := range r.consumers {
		wg.Add(1)
		go func(c *Consumer) {
			defer wg.Done()

			consumerErr := c.Consume(ctx)
			if consumerErr != nil {
				if err == nil {
					err = consumerErr
				}
				if !r.ContinueOnErrors {
					cancel()
				}
			}
		}(c)
	}

	wg.Wait()
	return err
}

func (r *Router) RegisterConsumer(topic string, consumer *Consumer) error {
	r.consumersMX.Lock()
	defer r.consumersMX.Unlock()

	if r.consumers == nil {
		r.consumers = make(map[string]*Consumer)
	}

	_, found := r.consumers[topic]
	if found {
		return fmt.Errorf("topic %s already has a consumer registered", topic)
	}

	r.consumers[topic] = consumer
	return nil
}

func (r *Router) RegisterHandler(topic string, subscriber Subscriber, handler MessageHandler) error {
	return r.RegisterSubscriber(topic, subscriber, UniqueResolver(handler))
}

func (r *Router) RegisterSubscriber(topic string, subscriber Subscriber, resolver HandlerResolver) error {
	return r.RegisterConsumer(topic, &Consumer{
		Subscriber:      subscriber,
		HandlerResolver: resolver,
		Unmarshaler:     r.Unmarshaler,
		OnReceive: func(message ReceivedMessage, err error) error {
			if f := r.OnReceive; f != nil {
				return r.OnReceive(topic, message, err)
			}
			return nil
		},
		OnUnregistered: func(message ReceivedMessage) error {
			if f := r.OnUnregistered; f != nil {
				return f(topic, message)
			}
			return nil
		},
		OnUnmarshal: func(message ReceivedMessage, err error) error {
			if f := r.OnUnmarshal; f != nil {
				return f(topic, message, err)
			}
			return nil
		},
		OnHandler: func(message ReceivedMessage, err error) error {
			if f := r.OnHandler; f != nil {
				return f(topic, message, err)
			}
			return nil
		},
		OnAck: func(message ReceivedMessage, err error) error {
			if f := r.OnAck; f != nil {
				return f(topic, message, err)
			}
			return nil
		},
		DisableAutoAck: r.DisableAutoAck,
	})
}
