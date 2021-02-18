package pubsub_test

import (
	"reflect"
	"testing"

	"github.com/hmoragrega/pubsub"
	"github.com/hmoragrega/pubsub/internal/stubs"
)

func TestNoOpUnmarshaller(t *testing.T) {
	m := pubsub.NoOpUnmarshaller()

	attributes := map[string]string{"key": "value"}
	msg := stubs.ReceivedMessageStub{
		IDFunc: func() string {
			return "id"
		},
		NameFunc: func() string {
			return "name"
		},
		KeyFunc: func() string {
			return "key"
		},
		BodyFunc: func() []byte {
			return []byte("body")
		},
		AttributesFunc: func() pubsub.Attributes {
			return attributes
		},
	}

	got, err := m.Unmarshal("foo", &msg)

	if err != nil {
		t.Fatalf("unexpected error; got %v, want nil", err)
	}
	if got, want := got.([]byte), msg.Body(); !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected data; got %v, want %v", got, want)
	}
}
