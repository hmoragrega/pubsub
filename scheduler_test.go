package pubsub_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/hmoragrega/pubsub"
	"github.com/hmoragrega/pubsub/internal/stubs"
	"github.com/hmoragrega/pubsub/marshaller"
)

var noOpMarshallerPublisher = pubsub.NewPublisher(pubsub.NoOpEnvelopePublisher(), &marshaller.ByteMarshaller{})

func TestPublisher(t *testing.T) {
	ctx := context.Background()

	t.Run("stores the message when schedule", func(t *testing.T) {
		var (
			m     stubs.StorageStub
			now   = time.Now()
			delay = time.Second
			topic = "foo"
			msg   = &pubsub.Message{Data: "data"}
		)

		m.ScheduleFunc = func(ctx context.Context, dueDate time.Time, gotTopic string, envelopes ...*pubsub.Envelope) error {
			expected := now.Add(delay)
			if !dueDate.Equal(expected) && !dueDate.After(expected) {
				return fmt.Errorf("due date is not in range: expected %v; got %v", expected, dueDate)
			}
			if gotTopic != topic {
				return fmt.Errorf("unexpected topic: expected %v; got %v", topic, gotTopic)
			}
			if len(envelopes) != 1 {
				return fmt.Errorf("no messages sent")
			}
			if got := string(envelopes[0].Body); got != msg.Data {
				return fmt.Errorf("unexpected message data: expected %v; got %v", msg.Data, got)
			}
			return nil
		}

		p := pubsub.NewSchedulerPublisher(noOpMarshallerPublisher, m)

		err := p.Delay(ctx, time.Second, "foo", msg)
		if err != nil {
			t.Fatalf("unexpected error delaying a message: %v", err)
		}
	})

	t.Run("returns marshaling failures when scheduling messages", func(t *testing.T) {
		var (
			m   stubs.StorageStub
			msg = &pubsub.Message{Data: 123} // invalid data
		)

		p := pubsub.NewSchedulerPublisher(noOpMarshallerPublisher, m)

		err := p.Schedule(ctx, time.Now(), "foo", msg)
		if err == nil || !strings.Contains(err.Error(), "invalid data type; expected string or byte slice, got int") {
			t.Fatalf("unexpected result scheduling a message: %v", err)
		}
	})

	t.Run("publishes due messages", func(t *testing.T) {
		var (
			m     stubs.StorageStub
			msg   = &pubsub.Envelope{Body: []byte("bar")}
			topic = "foo"
			feed  = make(chan pubsub.DueMessage, 1)
		)

		feed <- pubsub.DueMessage{Topic: topic, Envelope: msg}

		m.ConsumeDueFunc = func(ctx context.Context) (<-chan pubsub.DueMessage, error) {
			return feed, nil
		}
		m.PublishedFunc = func(ctx context.Context, got pubsub.DueMessage) error {
			if got.Envelope != msg {
				return fmt.Errorf("unexpected message: expected %v; got %v", msg, got.Envelope)
			}
			close(feed)
			return nil
		}

		p := pubsub.NewSchedulerPublisher(noOpMarshallerPublisher, m)

		err := p.PublishDue(ctx)
		if err != nil {
			t.Fatalf("unexpected error publishing due messages: %v", err)
		}
	})

	t.Run("returns an error if cannot consume due messages", func(t *testing.T) {
		var m stubs.StorageStub
		m.ConsumeDueFunc = func(ctx context.Context) (<-chan pubsub.DueMessage, error) {
			return nil, fmt.Errorf("dummy")
		}

		p := pubsub.NewSchedulerPublisher(noOpMarshallerPublisher, m)

		err := p.PublishDue(ctx)
		if err == nil || !strings.Contains(err.Error(), "dummy") {
			t.Fatalf("unexpected result consuming messages; got: %v", err)
		}
	})

	t.Run("stops if the consumer returns an error", func(t *testing.T) {
		var (
			m    stubs.StorageStub
			feed = make(chan pubsub.DueMessage, 1)
		)

		feed <- pubsub.DueMessage{Err: errorDummy}

		m.ConsumeDueFunc = func(ctx context.Context) (<-chan pubsub.DueMessage, error) {
			return feed, nil
		}

		p := pubsub.NewSchedulerPublisher(noOpMarshallerPublisher, m)

		err := p.PublishDue(ctx)
		if !errors.Is(err, errorDummy) {
			t.Fatalf("unexpected result consuming messages; got: %v", err)
		}
	})

	t.Run("stops if the publisher returns an error", func(t *testing.T) {
		var (
			m    stubs.StorageStub
			feed = make(chan pubsub.DueMessage, 1)
		)

		feed <- pubsub.DueMessage{Topic: "foo", Envelope: &pubsub.Envelope{}}

		m.ConsumeDueFunc = func(ctx context.Context) (<-chan pubsub.DueMessage, error) {
			return feed, nil
		}

		mp := pubsub.NewPublisher(pubsub.EnvelopePublisherFunc(func(ctx context.Context, topic string, envelopes ...*pubsub.Envelope) error {
			return errorDummy
		}), &marshaller.ByteMarshaller{})

		p := pubsub.NewSchedulerPublisher(mp, m)

		err := p.PublishDue(ctx)
		if !errors.Is(err, errorDummy) {
			t.Fatalf("unexpected result consuming messages; got: %v", err)
		}
	})

	t.Run("stops if the the consumer cannot be notified of the successful publishing", func(t *testing.T) {
		var (
			m    stubs.StorageStub
			feed = make(chan pubsub.DueMessage, 1)
		)

		feed <- pubsub.DueMessage{Topic: "foo", Envelope: &pubsub.Envelope{}}

		m.ConsumeDueFunc = func(ctx context.Context) (<-chan pubsub.DueMessage, error) {
			return feed, nil
		}
		m.PublishedFunc = func(ctx context.Context, message pubsub.DueMessage) error {
			return errorDummy
		}

		p := pubsub.NewSchedulerPublisher(noOpMarshallerPublisher, m)

		err := p.PublishDue(ctx)
		if !errors.Is(err, errorDummy) {
			t.Fatalf("unexpected result consuming messages; got: %v", err)
		}
	})
}
