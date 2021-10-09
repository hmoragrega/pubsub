package pubsub

import "context"

// NoOpPublisher skips publishing the messages without failures.
func NoOpPublisher() Publisher {
	return PublisherFunc(func(_ context.Context, _ string, _ ...*Message) error {
		return nil
	})
}

// NoOpEnvelopePublisher skips publishing the messages without failures.
func NoOpEnvelopePublisher() EnvelopePublisher {
	return EnvelopePublisherFunc(func(_ context.Context, _ string, _ ...*Envelope) error {
		return nil
	})
}
