package marshaller

import (
	"errors"
	"fmt"
	"reflect"

	"github.com/hmoragrega/pubsub"
	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/proto"
)

const protoText0x01 = "proto:t:0x01"

var (
	_ pubsub.Unmarshaller = (*ProtoTextMarshaller)(nil)
	_ pubsub.Marshaller   = (*ProtoTextMarshaller)(nil)
)

var (
	errNonProtoData      = errors.New("cannot marshal non-proto data type")
	errUnknownVersion    = errors.New("unknown version")
	errInstantiatingType = errors.New("cannot instantiating type")
	errUnmarshalling     = errors.New("cannot unmarshall message body")
)

// ProtoTextMarshaller marshals and unmarshals protocol
// buffer messages as the textproto format.
type ProtoTextMarshaller struct {
	MarshalOptions   prototext.MarshalOptions
	UnmarshalOptions prototext.UnmarshalOptions

	registry typeRegistry
}

// Register an event type by event name or topic.
func (m *ProtoTextMarshaller) Register(key string, v proto.Message) error {
	return m.registry.register(key, v)
}

// Marshal marshals a proto message.
func (m *ProtoTextMarshaller) Marshal(v interface{}) ([]byte, string, error) {
	pb, ok := v.(proto.Message)
	if !ok {
		return nil, "", fmt.Errorf("%w: %T", errNonProtoData, v)
	}

	b, err := m.MarshalOptions.Marshal(pb)
	return b, protoText0x01, err
}

// Unmarshal unmarshals the message body into the registered proto message.
func (m *ProtoTextMarshaller) Unmarshal(topic string, msg pubsub.ReceivedMessage) (*pubsub.Message, error) {
	if v := msg.Version(); v != protoText0x01 {
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

	err = m.UnmarshalOptions.Unmarshal(msg.Body(), data)
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

func (m *ProtoTextMarshaller) new(t reflect.Type) (v proto.Message, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%w: %s: %v", errInstantiatingType, t, r)
		}
	}()

	v = reflect.New(t).Interface().(proto.Message)

	return v, err
}
