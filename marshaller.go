package pubsub

type Marshaller interface {
	// Marshal the contents of the message.
	Marshal(data interface{}) (payload []byte, version string, err error)
}

// Unmarshaller will decode the received message.
// It should be aware of the version.
type Unmarshaller interface {
	Unmarshal(message ReceivedMessage) (*Message, error)
}

// UnmarshallerFunc will decode the received message.
// It should be aware of the version.
type UnmarshallerFunc func(message ReceivedMessage) (*Message, error)

func (f UnmarshallerFunc) Unmarshal(message ReceivedMessage) (*Message, error) {
	return f(message)
}
