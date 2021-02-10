package marshaller

import (
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/hmoragrega/pubsub"
)

const jsonVersion0x01 = "json:0x01"

var (
	_ pubsub.Unmarshaller = (*JSONMarshaller)(nil)
	_ pubsub.Marshaller   = (*JSONMarshaller)(nil)
)

// JSONMarshaller uses the standards json library
// to encode and decode message.
type JSONMarshaller struct {
	registry typeRegistry
}

// Register an event type by event name or topic.
func (m *JSONMarshaller) Register(key string, v interface{}) error {
	return m.registry.register(key, v)
}

// Marshal marshals the message data as JSON.
func (m *JSONMarshaller) Marshal(v interface{}) ([]byte, string, error) {
	b, err := json.Marshal(v)
	return b, jsonVersion0x01, err
}

// Unmarshal unmarshall the message body as JSON.
func (m *JSONMarshaller) Unmarshal(topic string, msg pubsub.ReceivedMessage) (*pubsub.Message, error) {
	if v := msg.Version(); v != jsonVersion0x01 {
		return nil, fmt.Errorf("%w: %s", errUnknownVersion, v)
	}

	t, err := m.registry.getType(msg.Name(), topic)
	if err != nil {
		return nil, err
	}

	data, err := m.new(t)
	if err != nil {
		return nil, err
	}

	err = json.Unmarshal(msg.Body(), data)
	if err != nil {
		return nil, fmt.Errorf("%w (%T): %v", errUnmarshalling, data, err)
	}

	return &pubsub.Message{
		ID:         msg.ID(),
		Name:       msg.Name(),
		Key:        msg.Key(),
		Attributes: msg.Attributes(),
		Data:       data,
	}, nil
}

func (m *JSONMarshaller) new(t reflect.Type) (v interface{}, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%w: %s: %v", errInstantiatingType, t, r)
		}
	}()

	v = reflect.New(t).Interface()

	return v, err
}
