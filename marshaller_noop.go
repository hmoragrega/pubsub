package pubsub

import (
	"errors"
	"fmt"
)

const (
	noOpStringVersion = "noop-s"
	noOpBytesVersion  = "noop-b"
)

var errInvalidDataType = errors.New("invalid data type")

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

func (m *NoOpMarshaller) Unmarshal(message ReceivedMessage) (*Message, error) {
	msg := &Message{
		ID:         message.ID(),
		Name:       message.Name(),
		Key:        message.Key(),
		Attributes: message.Attributes(),
	}

	switch v := message.Version(); v {
	case noOpStringVersion:
		msg.Data = string(message.Body())
	case noOpBytesVersion:
		msg.Data = message.Body()
	default:
		return nil, fmt.Errorf("message version not supported: %s", v)
	}

	return msg, nil
}
