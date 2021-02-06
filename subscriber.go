package pubsub

import "context"

// Subscriber consumes messages from a topic.
type Subscriber interface {
	// Subscribe to the topic and subscribe feeding messages.
	Subscribe() error

	// Next returns the next message in the topic.
	// It should block until the next message is ready
	// or the context is terminated.
	Next(ctx context.Context) (ReceivedMessage, error)

	// Stop stops consuming.
	Stop(ctx context.Context) error
}
