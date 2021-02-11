package pubsub

import (
	"context"
	"errors"
	"sync"
)

var (
	ErrNotReceivedMessage = errors.New("not a received message")
)

// MessageID a slice of bytes representing
// a unique message ID
type MessageID = string

// Attributes a list of message attributes.
type Attributes = map[string]string

// ReceivedMessage incoming message consumed by the subscriber.
type ReceivedMessage interface {
	// ID of the message, it should be unique.
	ID() string

	// Name of the message.
	Name() string

	// Key grouping key of the message.
	Key() string

	// Body body of the message.
	Body() []byte

	// Version of the envelope.
	Version() string

	// Message attributes.
	Attributes() Attributes

	// Ack acknowledges the message.
	Ack(ctx context.Context) error
}

// Message represent the information that
// we want to transmit.
type Message struct {
	// ID of the message, if empty a new one will
	// be generated automatically
	ID MessageID
	// Name of the message
	Name string
	// Key groups the message of the same type.
	// Different transports may try to guarantee
	// the order for messages with the same key.
	Key string
	// Data that we want to transmit.
	Data interface{}
	// Message attributes
	Attributes Attributes

	received ReceivedMessage
	mx       sync.RWMutex
}

// Ack acknowledges the message.
func (m *Message) Ack(ctx context.Context) error {
	m.mx.RLock()
	defer m.mx.RUnlock()

	if m.received != nil {
		return ErrNotReceivedMessage
	}

	return m.received.Ack(ctx)
}

// SetAttribute sets an attribute.
func (m *Message) SetAttribute(key, value string) {
	m.mx.Lock()
	defer m.mx.Unlock()

	if m.Attributes == nil {
		m.Attributes = make(Attributes)
	}

	m.Attributes[key] = value
}
