//+build integration

package aws

import (
	"context"
	"testing"
	"time"

	"github.com/hmoragrega/pubsub"
)

func TestPublisher(t *testing.T) {
	ctx := context.Background()

	env := &pubsub.Envelope{ID: "123", Version: "test:01", Body: []byte("data")}
	topicARN := createTestTopic(ctx, t, "combo-publisher")
	queueURL := createTestQueue(ctx, t, "combo-publisher")
	queueARN := MustGetResource(GetQueueARN(ctx, sqsTest, queueURL))
	subscribeTestTopic(ctx, t, topicARN, queueARN)
	Must(AttachQueueForwardingPolicy(ctx, sqsTest, queueURL, queueARN, topicARN))

	t.Run("it can publish using the direct queue URL", func(t *testing.T) {
		pub := NewPublisher(snsTest, sqsTest, nil)

		err := pub.Publish(ctx, queueURL, env)
		if err != nil {
			t.Fatalf("unexpected error publishing message; got %v", err)
		}

		requireReceivedEnvelope(t, queueURL, env)
	})

	t.Run("it can publish using the direct topic ARN", func(t *testing.T) {
		pub := NewPublisher(snsTest, sqsTest, nil)


		err := pub.Publish(ctx, topicARN, env)
		if err != nil {
			t.Fatalf("unexpected error publishing message; got %v", err)
		}

		requireReceivedEnvelope(t, queueURL, env)
	})

	t.Run("it can publish using a mapped queue URL", func(t *testing.T) {
		pub := NewPublisher(snsTest, sqsTest, map[string]string{
			"my-queue-alias": queueURL,
		})

		err := pub.Publish(ctx, "my-queue-alias", env)
		if err != nil {
			t.Fatalf("unexpected error publishing message; got %v", err)
		}

		requireReceivedEnvelope(t, queueURL, env)
	})

	t.Run("it can publish using a mapped topic ARN", func(t *testing.T) {
		pub := NewPublisher(snsTest, sqsTest, map[string]string{
			"my-topic-alias": topicARN,
		})

		err := pub.Publish(ctx, "my-topic-alias", env)
		if err != nil {
			t.Fatalf("unexpected error publishing message; got %v", err)
		}

		requireReceivedEnvelope(t, queueURL, env)
	})
}

func requireReceivedEnvelope(t *testing.T, queueURL string, env *pubsub.Envelope) {
	sub := NewSQSSubscriber(sqsTest, queueURL)

	msgs, err := sub.Subscribe()
	Must(err)

	t.Cleanup(func() {
		_ = sub.Stop(context.Background())
	})

	select {
	case <-time.NewTimer(time.Second).C:
		t.Fatal("timeout")
	case msg := <-msgs:
		if err := msg.Err; err != nil {
			t.Fatalf("unexpected error receiving message; got %v", err)
		}
		if got, want := msg.Message.ID(), env.ID; got != want {
			t.Fatalf("message ID is different; want %v, got %v", want, got)
		}
		if got, want := string(msg.Message.Body()), string(env.Body); got != want {
			t.Fatalf("message data is different; want %v, got %v", want, got)
		}
		if got, want := msg.Message.Version(), env.Version; got != want {
			t.Fatalf("message version is different; want %v, got %v", want, got)
		}

		if err := msg.Message.Ack(context.Background()); err != nil {
			t.Fatalf("unexpected error acknowledging the message: %v", err)
		}
	}
}