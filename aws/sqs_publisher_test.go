//+build integration

package aws

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/hmoragrega/pubsub"
)

func TestSQSPublisher_Publish(t *testing.T) {
	t.Run("publish successfully", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		queueURL := createTestQueue(ctx, t, "sqs-pub")
		sub, err := NewSQSSubscriber(sqsTest, queueURL).Subscribe()
		if err != nil {
			t.Fatal("error subscribing to queue", err)
		}

		env := &pubsub.Envelope{
			ID:      "123",
			Name:    "name",
			Key:     "key",
			Body:    []byte("body"),
			Version: "test",
		}
		validate := func(t *testing.T, msg pubsub.ReceivedMessage) {
			if b := msg.Body(); bytes.Compare(b, []byte("body")) != 0 {
				t.Fatalf("body is different, got %s", string(b))
			}
			if msg.Name() != "name" {
				t.Fatalf("name is different, got %s", msg.Name())
			}
			if msg.Key() != "key" {
				t.Fatalf("key is different, got %s", msg.Key())
			}
			if msg.ID() != "123" {
				t.Fatalf("ID is different, got %s", msg.ID())
			}
			if err := msg.Ack(ctx); err != nil {
				t.Fatal("error acknowledging message", err)
			}
		}

		t.Run("direct queue URL publish", func(t *testing.T) {
			pub := NewSQSDirectPublisher(sqsTest)
			err := pub.Publish(ctx, queueURL, env)
			if err != nil {
				t.Fatal("error publishing message", err)
			}

			select {
			case <-ctx.Done():
				t.Fatal("timeout receiving the message")
			case r := <-sub:
				if r.Err != nil {
					t.Fatal("error receiving message", err)
				}
				validate(t, r.Message)
			}
		})

		t.Run("queue map publisher", func(t *testing.T) {
			queueAlias := "foo-queue"
			pub := NewSQSPublisher(sqsTest, map[string]string{queueAlias: queueURL})

			err := pub.Publish(ctx, queueAlias, env)
			if err != nil {
				t.Fatal("error publishing message", err)
			}

			select {
			case <-ctx.Done():
				t.Fatal("timeout receiving the message")
			case r := <-sub:
				if r.Err != nil {
					t.Fatal("error receiving message", err)
				}
				validate(t, r.Message)
			}
		})
	})

	t.Run("queue not found", func(t *testing.T) {
		pub := NewSQSDirectPublisher(badSQS())
		err := pub.Publish(context.Background(), "foo", &pubsub.Envelope{})
		if !errors.Is(err, ErrQueueNotFound) {
			t.Fatalf("expected error %v; got %v", ErrQueueNotFound, err)
		}
	})

	t.Run("publish failure", func(t *testing.T) {
		pub := NewSQSPublisher(badSQS(), map[string]string{
			"foo": "https://foo-queue",
		})
		err := pub.Publish(context.Background(), "foo", &pubsub.Envelope{
			ID:      "123",
			Name:    "name",
			Key:     "key",
			Body:    []byte("body"),
			Version: "test",
		})
		if err == nil {
			t.Fatal("expected error publishing to SQS")
		}
	})
}
