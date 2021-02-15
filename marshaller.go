package pubsub

import "errors"

var ErrUnsupportedVersion = errors.New("unsupported version")

// Marshaller marshalls the contents of message data.
type Marshaller interface {
	Marshal(data interface{}) (payload []byte, version string, err error)
}

// Unmarshaller will decode the received message.
// It should be aware of the version.
type Unmarshaller interface {
	// Unmarshal unmarshalls the received message.
	// It must return ErrUnsupportedVersion if it does not support it.
	Unmarshal(topic string, message ReceivedMessage) (*Message, error)
}

// UnmarshallerFunc will decode the received message.
// It should be aware of the version.
type UnmarshallerFunc func(topic string, message ReceivedMessage) (*Message, error)

func (f UnmarshallerFunc) Unmarshal(topic string, message ReceivedMessage) (*Message, error) {
	return f(topic, message)
}

// NoOpUnmarshaller will pass the message data raw byte slice.
func NoOpUnmarshaller() Unmarshaller {
	return UnmarshallerFunc(func(topic string, message ReceivedMessage) (*Message, error) {
		return &Message{
			ID:         message.ID(),
			Name:       message.Name(),
			Key:        message.Key(),
			Attributes: message.Attributes(),
			Data:       message.Body(),
		}, nil
	})
}
