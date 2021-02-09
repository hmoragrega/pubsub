package pubsub

import (
	"context"
	"fmt"
)

type EnvelopePublisher interface {
	Publish(ctx context.Context, topic string, envelope Envelope) error
}

type Marshaller interface {
	// Marshal the contents of the message.
	Marshal(data interface{}) (payload []byte, version string, err error)
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

// Publisher can publish a message to the
// appropriate publisher based on the topic.
type Publisher struct {
	Publisher  EnvelopePublisher
	Marshaller Marshaller
}

func (p *Publisher) Publish(ctx context.Context, topic string, message Message) error {
	body, version, err := p.Marshaller.Marshal(message.Data)
	if err != nil {
		return fmt.Errorf("marshaller error: %w", err)
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
		Version:    version,
	})
}
