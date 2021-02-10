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
