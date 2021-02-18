package marshaller

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/hmoragrega/pubsub"
)

const jsonVersion0x01 = "json:0x01"

var ErrUnmarshalling = errors.New("failed to unmarshall")

var (
	_ pubsub.Unmarshaller = (*JSONMarshaller)(nil)
	_ pubsub.Marshaller   = (*JSONMarshaller)(nil)
)

// JSONMarshaller uses the standards json library
// to encode and decode message.
type JSONMarshaller struct {
	registry TypeRegistry
}

// Register an event type by event name or topic.
func (m *JSONMarshaller) Register(key string, v interface{}) error {
	return m.registry.Register(key, v)
}

// Marshal marshals the message data as JSON.
func (m *JSONMarshaller) Marshal(v interface{}) ([]byte, string, error) {
	b, err := json.Marshal(v)
	return b, jsonVersion0x01, err
}

// Unmarshal unmarshall the message body as JSON.
func (m *JSONMarshaller) Unmarshal(topic string, message pubsub.ReceivedMessage) (interface{}, error) {
	if v := message.Version(); v != jsonVersion0x01 {
		return nil, fmt.Errorf("%w: %s", pubsub.ErrUnsupportedVersion, v)
	}

	data, err := m.registry.GetNew(topic, message.Name())
	if err != nil {
		return nil, err
	}

	err = json.Unmarshal(message.Body(), data)
	if err != nil {
		return nil, fmt.Errorf("%w (%T): %v", ErrUnmarshalling, data, err)
	}

	return data, nil
}
