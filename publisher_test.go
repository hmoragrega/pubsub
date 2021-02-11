package pubsub_test

import (
	"context"
	"errors"
	"testing"

	"github.com/hmoragrega/pubsub"
	"github.com/hmoragrega/pubsub/internal/stubs"
)

func TestPublisher_PublishMarshallFailure(t *testing.T) {
	fakeErr := errors.New("foo")
	m := &stubs.MarshallerStub{
		MarshalFunc: func(data interface{}) ([]byte, string, error) {
			if data.(string) != "data" {
				t.Fatalf("unexpected data to marshall; got %+v", data)
			}
			return nil, "", fakeErr
		},
	}

	p := pubsub.NewPublisher(nil, m)

	err := p.Publish(context.Background(), "foo", &pubsub.Message{Data: "data"})
	if !errors.Is(err, fakeErr) {
		t.Fatalf("unexpected error result; got %v", err)
	}
}
