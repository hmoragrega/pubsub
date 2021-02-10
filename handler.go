package pubsub

import (
	"context"
	"errors"
	"fmt"
)

var ErrMissingHandler = errors.New("missing handler")

// Handler handles events.
type Handler interface {
	HandleMessage(ctx context.Context, message *Message) error
}

// HandlerFunc that handles an event
type HandlerFunc func(ctx context.Context, message *Message) error

func (f HandlerFunc) HandleMessage(ctx context.Context, message *Message) error {
	return f(ctx, message)
}

// Dispatcher is a message handler middleware that can be used to register
// different handlers for the same topic, based on the message name.
func Dispatcher(handlers map[string]Handler) HandlerFunc {
	return func(ctx context.Context, message *Message) error {
		h, ok := handlers[message.Name]
		if !ok {
			return fmt.Errorf("%w: %s", ErrMissingHandler, message.Name)
		}
		return h.HandleMessage(ctx, message)
	}
}
