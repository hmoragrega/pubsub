package pubsub

import (
	"encoding/json"
	"fmt"
	"reflect"
	"sync"
)

const jsonMarshallerVersion0x01 = "json:0x01"

// JSONMarshaller uses the standards json library
// to encode and decode message.
type JSONMarshaller struct {
	types map[string]reflect.Type
	mx    sync.RWMutex
}

// Register an event type by event name or topic.
func (m *JSONMarshaller) Register(key string, v interface{}) error {
	m.mx.Lock()
	defer m.mx.Unlock()

	if m.types == nil {
		m.types = make(map[string]reflect.Type)
	}

	if _, found := m.types[key]; found {
		return fmt.Errorf("type for key %q already registered", key)
	}

	m.types[key] = reflect.TypeOf(v).Elem()
	return nil
}

func (m *JSONMarshaller) Marshal(v interface{}) ([]byte, string, error) {
	b, err := json.Marshal(v)
	return b, jsonMarshallerVersion0x01, err
}

// BodyParser is message handler middleware that can decode
// the body of a message into an specific type using reflection
func (m *JSONMarshaller) Unmarshal(topic string, msg ReceivedMessage) (*Message, error) {
	if msg.Version() != jsonMarshallerVersion0x01 {
		return nil, fmt.Errorf("unknown message version %s", msg.Version())
	}

	t, err := m.getType(msg.Name(), topic)
	if err != nil {
		return nil, err
	}

	data, err := m.new(t)
	if err != nil {
		return nil, err
	}

	err = json.Unmarshal(msg.Body(), data)
	if err != nil {
		return nil, fmt.Errorf(
			"cannot decode mesage into type %T: %v",
			t.Name(),
			err,
		)
	}

	return &Message{
		ID:         msg.ID(),
		Name:       msg.Name(),
		Key:        msg.Key(),
		Attributes: msg.Attributes(),
		Data:       data,
	}, nil
}

func (m *JSONMarshaller) getType(name, topic string) (reflect.Type, error) {
	m.mx.RLock()
	defer m.mx.RUnlock()

	if t, ok := m.types[name]; ok {
		return t, nil
	}

	if t, ok := m.types[topic]; ok {
		return t, nil
	}

	return nil, fmt.Errorf("message type not regsitered: %s", name)
}

func (m *JSONMarshaller) new(t reflect.Type) (v interface{}, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("cannot get a new copy of %s: %v", t, r)
		}
	}()

	v = reflect.New(t).Interface()

	return v, err
}
