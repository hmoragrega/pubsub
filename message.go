package pubsub

import (
	"context"
	"errors"
	"sync"
)

var (
	ErrReceivedMessageNotAvailable = errors.New("received message not available")
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

	// Attributes message attributes.
	Attributes() Attributes

	// Ack acknowledges the message.
	Ack(ctx context.Context) error

	// NAck negative acknowledges the message.
	NAck(ctx context.Context) error

	// String prints the message.
	String() string
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

	ackOnce   sync.Once
	ackResult error
}

// NewMessageFromReceived builds a new message from a received one and
// it's unmarshalled body.
func NewMessageFromReceived(msg ReceivedMessage, data interface{}) *Message {
	return &Message{
		ID:         msg.ID(),
		Name:       msg.Name(),
		Key:        msg.Key(),
		Data:       data,
		Attributes: msg.Attributes(),
		received:   msg,
	}
}

// Ack acknowledges the message.
func (m *Message) Ack(ctx context.Context) error {
	m.ackOnce.Do(func() {
		if m.received == nil {
			m.ackResult = ErrReceivedMessageNotAvailable
			return
		}

		m.ackResult = m.received.Ack(ctx)
	})

	return m.ackResult
}

func (m *Message) NAck(ctx context.Context) error {
	m.ackOnce.Do(func() {
		if m.received == nil {
			m.ackResult = ErrReceivedMessageNotAvailable
			return
		}

		m.ackResult = m.received.NAck(ctx)
	})

	return m.ackResult
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
