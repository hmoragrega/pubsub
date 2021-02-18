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
	Unmarshal(topic string, message ReceivedMessage) (data interface{}, err error)
}

// UnmarshallerFunc will decode the received message.
// It should be aware of the version.
type UnmarshallerFunc func(topic string, message ReceivedMessage) (data interface{}, err error)

func (f UnmarshallerFunc) Unmarshal(topic string, message ReceivedMessage) (data interface{}, err error) {
	return f(topic, message)
}

// NoOpUnmarshaller will pass the message data raw byte slice
// without taking the version into account.
func NoOpUnmarshaller() Unmarshaller {
	return UnmarshallerFunc(func(topic string, message ReceivedMessage) (data interface{}, err error) {
		return message.Body(), nil
	})
}
