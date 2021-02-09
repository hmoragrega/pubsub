package pubsub

import "context"

// Next holds the next message
// in the subscription
type Next struct {
	Message ReceivedMessage
	Err     error
}

// Subscriber consumes messages from a topic.
type Subscriber interface {
	// Subscribe to the topic.
	Subscribe() (<-chan Next, error)

	// Stop stops consuming.
	Stop(ctx context.Context) error
}
