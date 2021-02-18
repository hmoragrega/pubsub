package pubsub_test

import (
	"context"
	"errors"
	"testing"

	"github.com/hmoragrega/pubsub"
	"github.com/hmoragrega/pubsub/internal/stubs"
)

func TestMessage_Ack(t *testing.T) {
	t.Run("fails if there is no received message", func(t *testing.T) {
		var m pubsub.Message

		err := m.Ack(context.Background())
		if !errors.Is(err, pubsub.ErrReceivedMessageNotAvailable) {
			t.Fatal("expected error", pubsub.ErrReceivedMessageNotAvailable)
		}
	})

	t.Run("success", func(t *testing.T) {
		m := pubsub.NewMessageFromReceived(&stubs.ReceivedMessageStub{
			IDFunc: func() string {
				return "123"
			},
			NameFunc: func() string {
				return "foo"
			},
			KeyFunc: func() string {
				return ""
			},
			AttributesFunc: func() pubsub.Attributes {
				return nil
			},
			AckFunc: func(ctx context.Context) error {
				return nil
			},
		}, nil)

		err := m.Ack(context.Background())
		if err != nil {
			t.Fatalf("unexpected error; got %v", err)
		}
	})
}
