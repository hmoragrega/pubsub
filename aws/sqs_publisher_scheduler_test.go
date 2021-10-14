//+build integration

package aws

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/hmoragrega/pubsub"
	"github.com/hmoragrega/pubsub/internal/stubs"
	"github.com/hmoragrega/pubsub/marshaller"
)

const maxDelay = 15 * time.Minute

func TestSQSSchedulerStorage(t *testing.T) {
	ctx := context.Background()

	queueURL := createTestQueue(ctx, t, "sqs-schedule")
	sub := NewSQSSubscriber(sqsTest, queueURL)
	msgs, err := sub.Subscribe()
	if err != nil {
		t.Fatal("error subscribing to queue", err)
	}

	t.Cleanup(func() {
		_ = sub.Stop(ctx)
	})

	t.Run("publish directly to SQS if the delay is 15 minutes or less", func(t *testing.T) {
		var s stubs.StorageStub
		ep := NewSQSDirectPublisher(sqsTest)
		ss := NewSQSSchedulerStorage(ep, &s)
		pub := pubsub.NewSchedulerPublisher(pubsub.NewPublisher(ep, &marshaller.ByteMarshaller{}), ss)

		s.ScheduleFunc = func(ctx context.Context, dueDate time.Time, _ string, _ ...*pubsub.Envelope) error {
			return fmt.Errorf("unexpected call to storage to schdule the message with delay %v", time.Now().Sub(dueDate))
		}

		err := pub.Delay(ctx, maxDelay, queueURL, &pubsub.Message{Data: "data"})
		if err != nil {
			t.Fatal("unexpected error result delaying a message", err)
		}
	})

	t.Run("messages are stored if the delay more than 15 minutes", func(t *testing.T) {
		var s stubs.StorageStub
		ep := NewSQSDirectPublisher(sqsTest)
		ss := NewSQSSchedulerStorage(ep, &s)
		pub := pubsub.NewSchedulerPublisher(pubsub.NewPublisher(ep, &marshaller.ByteMarshaller{}), ss)

		var stored bool
		s.ScheduleFunc = func(ctx context.Context, dueDate time.Time, _ string, _ ...*pubsub.Envelope) error {
			stored = true
			return nil
		}

		err := pub.Delay(ctx, maxDelay+time.Second, queueURL, &pubsub.Message{Data: "data"})
		if err != nil {
			t.Fatal("unexpected error result delaying a message", err)
		}
		if !stored {
			t.Fatal("message has not been stored")
		}
	})

	t.Run("message is retrieved at the correct time", func(t *testing.T) {
		if os.Getenv("AWS") != "true" {
			t.Skip("delay is not supported by go-aws mock")
		}

		var s stubs.StorageStub
		ep := NewSQSDirectPublisher(sqsTest)
		ss := NewSQSSchedulerStorage(ep, &s)
		pub := pubsub.NewSchedulerPublisher(pubsub.NewPublisher(ep, &marshaller.ByteMarshaller{}), ss)

		msgID := "123"
		delay := 3 * time.Second
		publishTime := time.Now()

		err := pub.Delay(ctx, delay, queueURL, &pubsub.Message{ID: msgID, Data: "foo"})
		if err != nil {
			t.Fatal("unexpected error result delaying a message", err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		t.Cleanup(cancel)

		select {
		case <-ctx.Done():
			t.Fatal("timeout waiting for message")
		case r := <-msgs:
			gotDelay := time.Now().Sub(publishTime).Round(time.Second)
			if err := r.Err; err != nil {
				t.Fatal("unexpected error receiving message", err)
			}
			if got := r.Message.ID(); got != msgID {
				t.Fatalf("unexpected message ID, want %v, got %v", got, msgID)
			}
			if gotDelay != delay {
				t.Fatalf("unexpected delay, want %v, got %v", delay, gotDelay)
			}
		}
	})
}
