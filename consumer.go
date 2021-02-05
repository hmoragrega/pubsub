package pubsub

import "C"
import (
	"context"
	"errors"
)

// Subscriber consumes messages from a topic.
type Subscriber interface {
	// Start consuming the messages
	Start() error

	// Next returns the next message in the topic.
	// It should block until the next message is ready
	// or the context is terminated.
	Next(ctx context.Context) (ReceivedMessage, error)

	// Stop stops consuming.
	Stop(ctx context.Context) error
}

// MessageHandler handles events.
type MessageHandler interface {
	HandleMessage(ctx context.Context, message *Message) error
}

// MessageHandlerFunc that handles an event
type MessageHandlerFunc func(ctx context.Context, message *Message) error

func (f MessageHandlerFunc) HandleMessage(ctx context.Context, message *Message) error {
	return f(ctx, message)
}

// HandlerResolver is able to return the correct
// handler for a received message
type HandlerResolver interface {
	// Resolve return the message handler. If it does not
	// now the handler it will return false in the second
	// parameter.
	Resolve(message ReceivedMessage) (MessageHandler, bool)
}

type HandlerResolverFunc func(message ReceivedMessage) (MessageHandler, bool)

func (f HandlerResolverFunc) Resolve(message ReceivedMessage) (MessageHandler, bool) {
	return f(message)
}

// UniqueResolver will return always the same message handler.
// UniqueResolver
func UniqueResolver(handler MessageHandler) HandlerResolverFunc {
	return func(_ ReceivedMessage) (MessageHandler, bool) {
		return handler, true
	}
}

// Dispatcher will solve the handler based on a map using the
// name as the key
func Dispatcher(handlers map[string]MessageHandler) HandlerResolverFunc {
	return func(msg ReceivedMessage) (MessageHandler, bool) {
		return handlers[msg.Name()], true
	}
}

// Unmarshaler will decode the received message.
// It should be aware of the version.
type Unmarshaler interface {
	Unmarshal(message ReceivedMessage) (*Message, error)
}

// Consumer acts like a normal consumer but routes
// the messages to different handlers based on the
// received message name.
type Consumer struct {
	// Message consumer
	Subscriber
	// Message handlers
	HandlerResolver HandlerResolver
	// Message handlers
	Unmarshaler Unmarshaler
	// Optional callback invoked when the consumer
	// reports an error.
	OnReceive func(message ReceivedMessage, err error)
	// Optional callback invoked when the consumer
	// the message has no handler associated
	OnUnregistered func(message ReceivedMessage)
	// Optional callback invoked when the received message
	// cannot be unmarshaled into a message.
	OnUnmarshal func(message ReceivedMessage, err error)
	// OnHandlerError callback invoked when the handler
	// returns an error.
	OnHandler func(message ReceivedMessage, err error)
	// Optional callback invoked when the handled
	// message cannot be acknowledged
	OnAck func(message ReceivedMessage, err error)
	// Disables automatic acknowledgement of the messages
	// The handler will be responsible for it.
	DisableAutoAck bool
}

// Consume keeps consuming events and dispatching them
// to the correct event handler.
//
// It does not stop on errors, set up the "OnError"
// optional callback to receive the errors coming
// from the consumer to react to them.
//
// To stop the dispatcher, terminate the context.
func (c *Consumer) Consume(ctx context.Context) {
	for {
		msg, err := c.Next(ctx)
		if errors.Is(err, context.Canceled) {
			return
		}
		if f := c.OnReceive; f != nil {
			f(msg, err)
		}
		if err != nil {
			continue
		}

		h, ok := c.HandlerResolver.Resolve(msg)
		if !ok {
			if f := c.OnUnregistered; f != nil {
				f(msg)
			}
			continue
		}

		message, err := c.Unmarshaler.Unmarshal(msg)
		if f := c.OnUnmarshal; f != nil {
			f(msg, err)
		}
		if err != nil {
			continue
		}

		err = h.HandleMessage(ctx, message)
		if f := c.OnHandler; f != nil {
			f(msg, err)
		}
		if err != nil {
			continue
		}

		if c.DisableAutoAck {
			continue
		}
		err = msg.Ack(ctx)
		if f := c.OnAck; f != nil {
			f(msg, err)
		}
	}
}
