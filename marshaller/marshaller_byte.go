package marshaller

import (
	"fmt"

	"github.com/hmoragrega/pubsub"
)

const (
	byteStringVersion = "byte:s"
	byteSliceVersion  = "byte:b"
)

var (
	_ pubsub.Unmarshaller = (*ByteMarshaller)(nil)
	_ pubsub.Marshaller   = (*ByteMarshaller)(nil)
)

// ByteMarshaller accepts payloads as string
// or byte slice and returns it as is.
type ByteMarshaller struct{}

// Marshal marshalls a string or byte slice payloads.
func (m *ByteMarshaller) Marshal(data interface{}) ([]byte, string, error) {
	switch data.(type) {
	case string:
		return []byte(data.(string)), byteStringVersion, nil
	case []byte:
		return data.([]byte), byteSliceVersion, nil
	}

	return nil, "", fmt.Errorf("%w; expected string or byte slice, got %T", ErrInvalidDataType, data)
}

// Unmarshal unmarshals a string or byte slice.
func (m *ByteMarshaller) Unmarshal(_ string, message pubsub.ReceivedMessage) (interface{}, error) {
	switch v := message.Version(); v {
	case byteStringVersion:
		return string(message.Body()), nil
	case byteSliceVersion:
		return message.Body(), nil
	default:
		return nil, fmt.Errorf("%w: %s", pubsub.ErrUnsupportedVersion, v)
	}
}
