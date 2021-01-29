package pubsub

import "context"

// MessageID a slice of bytes representing
// a unique message ID
type MessageID = []byte

// Attributes a list of message attributes.
type Attributes = map[string]string

// Message incoming message consumed by the subscriber.
type Message interface {
	// ID of the message, it should be unique.
	ID() string

	// Key grouping key of the message.
	Key() string

	// Body body of the message.
	Body() []byte

	// Attribute returns one attribute of
	// the message. The second parameter
	// indicates if the attribute was set
	// or not.
	Attribute(key string) (string, bool)

	// Returns the whole list of attributes.
	Attributes() Attributes

	// Ack acknowledges the message.
	Ack(ctx context.Context) error
}


// Envelope is a struct that wraps the
// message information for publishing.
type Envelope struct {
	ID         MessageID
	Key        string
	Body       []byte
	Attributes Attributes
}

// NewEnvelope constructor helper for creating new envelopes
// initializing the attributes map.
func NewEnvelope(key string, body []byte) *Envelope {
	return NewEnvelopeWithAttributes(key, body, make(map[string]string))
}

// NewEnvelope constructor helper for creating new envelopes
// accepting extra message attributes.
func NewEnvelopeWithAttributes(key string, body []byte, attributes map[string]string) *Envelope {
	return &Envelope{
		ID:         NewID(),
		Key:        key,
		Body:       body,
		Attributes: attributes,
	}
}
