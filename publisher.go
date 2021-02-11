package pubsub

import (
	"context"
	"fmt"
)

// Publisher describes the top level method to publish messages.
type Publisher interface {
	Publish(ctx context.Context, topic string, messages ...*Message) error
}

// PublisherFunc is a function that can publish messages.
type PublisherFunc func(ctx context.Context, topic string, envelopes ...*Message) error

// Publish publishes messages invoking the function.
func (f PublisherFunc) Publish(ctx context.Context, topic string, envelopes ...*Message) error {
	return f(ctx, topic, envelopes...)
}

// EnvelopePublisher publish envelopes where the data has already been
// marshalled.
//
// You can also use this interface directly is you want to handle the
// marshalling yourself, combined with the NoOpMarshaller for the
// router.
type EnvelopePublisher interface {
	Publish(ctx context.Context, topic string, envelopes ...*Envelope) error
}

// EnvelopePublisherFunc is a function that can publish envelopes.
type EnvelopePublisherFunc func(ctx context.Context, topic string, envelopes ...*Envelope) error

// Publish publishes envelopes invoking the function.
func (f EnvelopePublisherFunc) Publish(ctx context.Context, topic string, envelopes ...*Envelope) error {
	return f(ctx, topic, envelopes...)
}

// Envelope holds the data that need to be transmitted.
type Envelope struct {
	ID         MessageID
	Name       string
	Key        string
	Body       []byte
	Version    string
	Attributes Attributes
}

// MarshallerPublisher will marshall a message and publish a message
// delegate the publishing of the envelope.
type MarshallerPublisher struct {
	publisher  EnvelopePublisher
	marshaller Marshaller
}

// NewPublisher creates a new marshaller publisher.
func NewPublisher(publisher EnvelopePublisher, marshaller Marshaller) *MarshallerPublisher {
	return &MarshallerPublisher{
		publisher:  publisher,
		marshaller: marshaller,
	}
}

// Publish a message to the given topic.
func (p *MarshallerPublisher) Publish(ctx context.Context, topic string, messages ...*Message) error {
	envelopes := make([]*Envelope, len(messages))
	for i, m := range messages {
		body, version, err := p.marshaller.Marshal(m.Data)
		if err != nil {
			return fmt.Errorf("marshaller error (%d): %w", i, err)
		}
		id := m.ID
		if len(m.ID) == 0 {
			id = NewID()
		}
		envelopes[i] = &Envelope{
			ID:         id,
			Name:       m.Name,
			Key:        m.Key,
			Attributes: m.Attributes,
			Body:       body,
			Version:    version,
		}
	}

	return p.publisher.Publish(ctx, topic, envelopes...)
}

// PublisherHandler handles events and generates new
// messages that should be published.
type PublisherHandler interface {
	HandleMessage(ctx context.Context, message *Message) ([]*Message, error)
}

// PublisherHandlerFunc function that can handle a message.
type PublisherHandlerFunc func(ctx context.Context, message *Message) ([]*Message, error)

// HandleMessage handles the message with the function.
func (f PublisherHandlerFunc) HandleMessage(ctx context.Context, message *Message) ([]*Message, error) {
	return f(ctx, message)
}

// Handler is a helper that publishes messages generated by other handlers.
func (p *MarshallerPublisher) Handler(topic string, handler PublisherHandler) Handler {
	return HandlerFunc(func(ctx context.Context, message *Message) error {
		messages, err := handler.HandleMessage(ctx, message)
		if err != nil {
			return err
		}

		return p.Publish(ctx, topic, messages...)
	})
}
