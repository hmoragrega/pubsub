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
		rm := stubs.NewNoOpReceivedMessage()
		rm.IDFunc = func() string {
			return "123"
		}
		rm.NameFunc = func() string {
			return "foo"
		}
		m := pubsub.NewMessageFromReceived(rm, nil)

		err := m.Ack(context.Background())
		if err != nil {
			t.Fatalf("unexpected error; got %v", err)
		}
	})
}
