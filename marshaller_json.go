package pubsub

import (
	"encoding/json"
	"fmt"
	"reflect"
)

const jsonMarshallerVersion0x01 = "json-marshaller:0x01"

// JSONMarshaller uses the standards json library
// to encode and decode message.
type JSONMarshaller struct {
	types map[string]reflect.Type
}

func (m *JSONMarshaller) Version() string {
	return jsonMarshallerVersion0x01
}

// Register an event type. Not thread-safe.
func (m *JSONMarshaller) Register(name string, v interface{}) {
	if m.types == nil {
		m.types = make(map[string]reflect.Type)
	}
	m.types[name] = reflect.TypeOf(v).Elem()
}

func (m *JSONMarshaller) Marshal(v interface{}) ([]byte, error) {
	return json.Marshal(v)
}

// BodyParser is message handler middleware that can decode
// the body of a message into an specific type using reflection
func (m *JSONMarshaller) Unmarshal(msg ReceivedMessage) (*Message, error) {
	if msg.Version() != jsonMarshallerVersion0x01 {
		return nil, fmt.Errorf("unknown message version %s", msg.Version())
	}

	name := msg.Name()
	t, ok := m.types[msg.Name()]
	if !ok {
		return nil, fmt.Errorf("message type not regsitered: %s", name)
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

func (m *JSONMarshaller) new(t reflect.Type) (v interface{}, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("cannot get a new copy of %s: %v", t, r)
		}
	}()

	v = reflect.New(t).Interface()

	return v, err
}
