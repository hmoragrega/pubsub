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
	if got, want := got.ID, msg.ID(); got != want {
		t.Fatalf("unexpected id; got %v, want %v", got, want)
	}
	if got, want := got.Name, msg.Name(); got != want {
		t.Fatalf("unexpected name; got %v, want %v", got, want)
	}
	if got, want := got.Key, msg.Key(); got != want {
		t.Fatalf("unexpected key; got %v, want %v", got, want)
	}
	if got, want := got.Data.([]byte), msg.Body(); !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected data; got %v, want %v", got, want)
	}
	if got, want := got.Attributes, msg.Attributes(); !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected id; got %v, want %v", got, want)
	}
}
