package marshaller

import (
	"errors"
	"fmt"

	"github.com/hmoragrega/pubsub"
)

const (
	noOpStringVersion = "noop:s"
	noOpBytesVersion  = "noop:b"
)

var errInvalidDataType = errors.New("invalid data type")

var (
	_ pubsub.Unmarshaller = (*NoOpMarshaller)(nil)
	_ pubsub.Marshaller   = (*NoOpMarshaller)(nil)
)

// NoOpMarshaller accepts payloads as string
// or byte slice and returns it as is.
type NoOpMarshaller struct{}

func (m *NoOpMarshaller) Marshal(data interface{}) ([]byte, string, error) {
	switch data.(type) {
	case string:
		return []byte(data.(string)), noOpStringVersion, nil
	case []byte:
		return data.([]byte), noOpBytesVersion, nil
	}

	return nil, "", fmt.Errorf("%w; expected string or byte slice, got %T", errInvalidDataType, data)
}

func (m *NoOpMarshaller) Unmarshal(_ string, message pubsub.ReceivedMessage) (*pubsub.Message, error) {
	var data interface{}
	switch v := message.Version(); v {
	case noOpStringVersion:
		data = string(message.Body())
	case noOpBytesVersion:
		data = message.Body()
	default:
		return nil, fmt.Errorf("%w: %s", errUnknownVersion, v)
	}

	return &pubsub.Message{
		ID:         message.ID(),
		Name:       message.Name(),
		Key:        message.Key(),
		Attributes: message.Attributes(),
		Data:       data,
	}, nil
}
