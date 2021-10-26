package pubsub

import (
	"context"
	"errors"
	"sync"
	"time"
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

	// Body of the message.
	Body() []byte

	// Version of the envelope.
	Version() string

	// Attributes message attributes.
	Attributes() Attributes

	// Ack acknowledges the message.
	Ack(ctx context.Context) error

	// NAck negative acknowledges the message.
	NAck(ctx context.Context) error

	// ReSchedule puts the message back in the same topic
	// to be available again after a certain delay
	// And Ack or Nack is expected to happen at this point,
	// although not required.
	ReSchedule(ctx context.Context, delay time.Duration) error

	// ReceivedCount returns the number of times this message
	// has been delivered.
	// If the messaging system does not support this information
	// it should be 0.
	ReceivedCount() int

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
	// ReceivedCount returns the number of times this message
	// has been delivered.
	ReceivedCount int
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
		ID:            msg.ID(),
		Name:          msg.Name(),
		Key:           msg.Key(),
		Data:          data,
		ReceivedCount: msg.ReceivedCount(),
		Attributes:    msg.Attributes(),
		received:      msg,
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

// ReSchedule puts the message back again in the topic/queue to be
// available after a certain delay.
//
// Different implementations may need to act accordingly and
// ack or n/ack the message.
//
// The common scenario is that a message has been failed to be processed
// and, we don't want to receive it immediately, probably applying
// a backoff strategy with increased delay times.
func (m *Message) ReSchedule(ctx context.Context, delay time.Duration) error {
	m.ackOnce.Do(func() {
		if m.received == nil {
			m.ackResult = ErrReceivedMessageNotAvailable
			return
		}

		m.ackResult = m.received.ReSchedule(ctx, delay)
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
