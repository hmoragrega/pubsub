package pubsub

// Unmarshaler will decode the received message.
// It should be aware of the version.
type Unmarshaler interface {
	Unmarshal(message ReceivedMessage) (*Message, error)
}

// UnmarshalerFunc will decode the received message.
// It should be aware of the version.
type UnmarshalerFunc func(message ReceivedMessage) (*Message, error)

func (f UnmarshalerFunc) Unmarshal(message ReceivedMessage) (*Message, error) {
	return f(message)
}
