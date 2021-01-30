package pubsub

import (
	"context"
	"errors"
)

type EnvelopePublisher interface {
	Publish(ctx context.Context, topic string, envelope Envelope) error
}

type Marshaler interface {
	// Marshal the contents of the message.
	Marshal(data interface{}) ([]byte, error)
	// Version identifies the marshaller version
	// allows changing the marshaller implementation
	// while being able to consume older messages.
	Version() string
}

// Envelope holds the data that need to be transmitted.
// version 1.
type Envelope struct {
	ID         MessageID
	Name       string
	Key        string
	Body       []byte
	Version    string
	Attributes Attributes
}

// Publisher can publish a message to the
// appropriate publisher based on the topic.
type Publisher struct {
	Publisher EnvelopePublisher
	Marshaler Marshaler
}

func (p *Publisher) Publish(ctx context.Context, topic string, message Message) error {
	if message.Name == "" {
		return errors.New("missing message name")
	}

	body, err := p.Marshaler.Marshal(message.Data)
	if err != nil {
		return err
	}

	id := message.ID
	if len(message.ID) == 0 {
		id = NewID()
	}

	return p.Publisher.Publish(ctx, topic, Envelope{
		ID:         id,
		Name:       message.Name,
		Key:        message.Key,
		Attributes: message.Attributes,
		Body:       body,
		Version:    p.Marshaler.Version(),
	})
}
