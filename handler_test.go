package pubsub

import (
	"context"
	"errors"
	"testing"
)

func TestDispatcher_MissingHandler(t *testing.T) {
	d := Dispatcher(nil)

	err := d.HandleMessage(context.Background(), &Message{})

	if !errors.Is(err, ErrMissingHandler) {
		t.Fatalf("expected missing handler error; got %+v", err)
	}
}

func TestRecoverer(t *testing.T) {
	wrappedHandler := WrapHandler(HandlerFunc(func(ctx context.Context, message *Message) error {
		panic("problem")
	}), Recoverer)

	err := wrappedHandler.HandleMessage(context.Background(), nil)
	if err == nil {
		t.Fatalf("expected error from recovered panic; got %v", err)
	}
}

func TestAcknowledge(t *testing.T) {
	h := Acknowledge(HandlerFunc(func(ctx context.Context, message *Message) error {
		return nil
	}))

	var msg Message

	err := h.HandleMessage(context.Background(), &msg)
	if err != nil {
		t.Fatalf("unexpected error handling the message; got %v", err)
	}
	if err := msg.Ack(context.Background()); err != ErrReceivedMessageNotAvailable {
		t.Fatalf("expected error acking the message; got %v", err)
	}
}
