package pubsub_test

import (
	"context"
	"errors"
	"testing"

	"github.com/hmoragrega/pubsub"
	"github.com/hmoragrega/pubsub/internal/stubs"
)

func TestMessage_Ack(t *testing.T) {
	var m pubsub.Message

	err := m.Ack(context.Background())
	if !errors.Is(err, pubsub.ErrReceivedMessageNotAvailable) {
		t.Fatal("expected error", pubsub.ErrReceivedMessageNotAvailable)
	}

	m.AttachReceivedMessage(&stubs.ReceivedMessageStub{AckFunc: func(ctx context.Context) error {
		return nil
	}})

	err = m.Ack(context.Background())
	if err != nil {
		t.Fatalf("unexpected error; got %v", err)
	}
}
