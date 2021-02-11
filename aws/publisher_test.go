//+build integration

package aws

import (
	"context"
	"errors"
	"testing"

	"github.com/hmoragrega/pubsub"
)

func TestPublisher_PublishFailures(t *testing.T) {
	t.Run("topic not found", func(t *testing.T) {
		var pub Publisher
		err := pub.Publish(context.Background(), "foo", &pubsub.Envelope{})
		if !errors.Is(err, ErrTopicNotFound) {
			t.Fatalf("expected error %v; got %v", ErrTopicNotFound, err)
		}
	})

	t.Run("publish failure", func(t *testing.T) {
		pub := Publisher{
			sns: badSNS(),
			topicARNs: map[string]string{
				"foo": "arn-foo",
			},
		}
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
