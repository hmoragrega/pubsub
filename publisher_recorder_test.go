package pubsub

import (
	"context"
	"testing"
)

func TestPublisherRecorder(t *testing.T) {
	ctx := context.Background()
	p := NewPublisherRecorder(NoOpPublisher())

	err := p.Publish(ctx, "topic-1", &Message{
		ID: "123",
	}, &Message{
		ID: "456",
	})
	if err != nil {
		t.Fatal("unexpected error publishing", err)
	}

	err = p.Publish(ctx, "topic-2", &Message{
		ID: "987",
	}, &Message{
		ID: "654",
	})
	if err != nil {
		t.Fatal("unexpected error publishing", err)
	}

	if got, want := len(p.Messages()), 4; got != want {
		t.Fatalf("unexpected number of published records; got %+v, want %+v", got, want)
	}

	for i, want := range []struct {
		topic string
		id    string
	}{
		{"topic-1", "123"},
		{"topic-1", "456"},
		{"topic-2", "987"},
		{"topic-2", "654"},
	} {
		if got, want := p.Messages()[i].Topic, want.topic; got != want {
			t.Fatalf("unexpected message topic at position %d; got %s, want %s", i, got, want)
		}
		if got, want := p.Messages()[i].Message.ID, want.id; got != want {
			t.Fatalf("unexpected message id at position %d; got %s, want %s", i, got, want)
		}
	}

	for i, want := range []string{"123", "456"} {
		if got := p.MessagesMap()["topic-1"][i].ID; got != want {
			t.Fatalf("unexpected message id at position %d; got %s, want %s", i, got, want)
		}
	}

	for i, want := range []string{"987", "654"} {
		if got := p.TopicMessages("topic-2")[i].ID; got != want {
			t.Fatalf("unexpected message id at position %d; got %s, want %s", i, got, want)
		}
	}

	p.Reset()

	if got, want := len(p.Messages()), 0; got != want {
		t.Fatalf("unexpected number of published records; got %+v, want %+v", got, want)
	}
	if got, want := len(p.MessagesMap()), 0; got != want {
		t.Fatalf("unexpected number of published records in the map; got %+v, want %+v", got, want)
	}
}
