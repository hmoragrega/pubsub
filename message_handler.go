package pubsub

import (
	"context"
	"errors"
	"fmt"
)

var ErrMissingHandler = errors.New("missing handler")

// MessageHandler handles events.
type MessageHandler interface {
	HandleMessage(ctx context.Context, message *Message) error
}

// MessageHandlerFunc that handles an event
type MessageHandlerFunc func(ctx context.Context, message *Message) error

func (f MessageHandlerFunc) HandleMessage(ctx context.Context, message *Message) error {
	return f(ctx, message)
}

// Dispatcher is a message handler middleware that can be used to register
// different handlers for the same topic, based on the event name.
func Dispatcher(handlers map[string]MessageHandler) MessageHandlerFunc {
	return func(ctx context.Context, message *Message) error {
		h, ok := handlers[message.Name]
		if !ok {
			return fmt.Errorf("%w: %s", ErrMissingHandler, message.Name)
		}
		return h.HandleMessage(ctx, message)
	}
}
