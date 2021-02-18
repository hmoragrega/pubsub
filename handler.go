package pubsub

import (
	"context"
	"errors"
	"fmt"
)

// ErrMissingHandler is fired when the dispatcher has not handler
// registered for the message name.
var ErrMissingHandler = errors.New("missing handler")

// Handler handles events.
type Handler interface {
	HandleMessage(ctx context.Context, message *Message) error
}

// HandlerFunc that handles an event
type HandlerFunc func(ctx context.Context, message *Message) error

// HandleMessage handles the message using the function.
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

// Recoverer will prevent panics in the handler
func Recoverer(next Handler) Handler {
	return HandlerFunc(func(ctx context.Context, message *Message) (err error) {
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("panic recovered: %v", r)
			}
		}()
		err = next.HandleMessage(ctx, message)
		return
	})
}

// Wrapper will wrap the handler in the given middlewares.
func Wrapper(handler Handler, middlewares ...func(Handler) Handler) HandlerFunc {
	return func(ctx context.Context, message *Message) (err error) {
		for _, mw := range middlewares {
			handler = mw(handler)
		}
		return handler.HandleMessage(ctx, message)
	}
}
