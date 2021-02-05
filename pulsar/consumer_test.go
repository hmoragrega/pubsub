//+build integration

package pulsar

import (
	"context"
	"errors"
	"log"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/hmoragrega/workers"
	"github.com/hmoragrega/workers/middleware"

	"github.com/hmoragrega/pubsub"
)

func foo() {
	ctx := context.Background()

	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL: "pulsar://localhost:6650",
	})
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	consumer := NewConsumer(client, pulsar.ConsumerOptions{
		Topic:            "topic",
		SubscriptionName: "my-sub-name",
		Type:             pulsar.Shared,
	})

	subscriber := pubsub.Subscriber{
		Pool: workers.New(middleware.Retry(2)),
	}

	err = subscriber.Start(consumer)
	if err != nil {
		log.Fatal(err)
	}

	defer func() {
		_ = subscriber.CloseWIthTimeout(10 * time.Second)
	}()

	for {
		m, err := subscriber.Next(ctx)
		if errors.Is(err, context.Canceled) {
			return
		}
		if err != nil {
			// whatever you choose!
		}
		// process message

		// main context could have been cancelled,
		// we should ack the message
		ackCtx := context.Background()
		_ = m.Ack(ackCtx)
	}
}
