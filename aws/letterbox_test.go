//+build integration

package aws

import (
	"context"
	"testing"
	"time"

	"github.com/hmoragrega/pubsub"
)

type sumRequest struct {
	A int
	B int
}

type sumResponse struct {
	X int
}

type subtractRequest struct {
	A int
	B int
}

type subtractResponse struct {
	X int
}

func TestInbox(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 99999*time.Second)
	defer cancel()

	var (
		instanceTopic       = "letterbox-test-instance-topic"
		mathSvcTopic        = "letterbox-test-math-svc-topic"
		instanceTopicARN    = createTestTopic(ctx, t, instanceTopic)
		mathServiceTopicARN = createTestTopic(ctx, t, mathSvcTopic)
		instanceQueueURL    = createTestQueue(ctx, t, "letterbox-test-instance-queue")
		mathSvcQueueURL     = createTestQueue(ctx, t, "letterbox-test-math-svc-queue")
		_                   = subscribeTestTopic(ctx, t, instanceTopicARN, instanceQueueURL)
		_                   = subscribeTestTopic(ctx, t, mathServiceTopicARN, mathSvcQueueURL)
		jsonMarshaler       pubsub.JSONMarshaller
	)

	publisher := &pubsub.Publisher{
		Publisher: NewPublisher(snsTest, map[string]string{
			mathSvcTopic: mathServiceTopicARN,
		}),
		Marshaler: &jsonMarshaler,
	}

	var (
		sumRequestEventName       = "sum-request"
		sumResponseEventName      = "sum-response"
		subtractRequestEventName  = "subtract-request"
		subtractResponseEventName = "subtract-response"
	)

	jsonMarshaler.Register(sumRequestEventName, &sumRequest{})
	jsonMarshaler.Register(sumResponseEventName, &sumResponse{})
	jsonMarshaler.Register(subtractRequestEventName, &subtractRequest{})
	jsonMarshaler.Register(subtractResponseEventName, &subtractResponse{})

	letterbox := &pubsub.Letterbox{
		Publisher: publisher,
		Topic:     instanceTopicARN,
	}

	// math service can do some hard math.
	mathSvcSubscriber := &pubsub.Consumer{
		Subscriber: NewSubscriber(sqsTest, mathSvcQueueURL),
		HandlerResolver: pubsub.Dispatcher(map[string]pubsub.MessageHandler{
			sumRequestEventName: pubsub.MessageHandlerFunc(func(ctx context.Context, request *pubsub.Message) error {
				req := request.Data.(*sumRequest)
				x := req.A + req.B
				return letterbox.Response(ctx, request, &pubsub.Message{
					Name: sumResponseEventName,
					Data: &sumResponse{X: x},
				})
			}),
			subtractRequestEventName: pubsub.MessageHandlerFunc(func(ctx context.Context, request *pubsub.Message) error {
				req := request.Data.(*subtractRequest)
				x := req.A - req.B
				return letterbox.Response(ctx, request, &pubsub.Message{
					Name: subtractResponseEventName,
					Data: &subtractResponse{X: x},
				})
			}),
		}),
		Unmarshaler: &jsonMarshaler,
	}

	// the letterbox acts as message handler for the responses.
	instanceConsumer := &pubsub.Consumer{
		Subscriber: NewSubscriber(sqsTest, instanceQueueURL),
		HandlerResolver: pubsub.Dispatcher(map[string]pubsub.MessageHandler{
			sumResponseEventName:      letterbox,
			subtractResponseEventName: letterbox,
		}),
		Unmarshaler: &jsonMarshaler,
	}

	consumerGroup := pubsub.ConsumerGroup{Consumers: []*pubsub.Consumer{
		mathSvcSubscriber,
		instanceConsumer,
	}}
	consumerGroup.MustStartAll(ctx)

	consumersStopped := make(chan struct{})
	go func() {
		consumerGroup.Consume(ctx)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if errs := consumerGroup.Stop(ctx); len(errs) > 0 {
			t.Error("cannot stop all consumers", errs)
		}
		close(consumersStopped)
	}()

	// do a sum
	res, err := letterbox.Request(ctx, mathSvcTopic, pubsub.Message{
		Name: sumRequestEventName,
		Data: &sumRequest{A: 9, B: 5},
	})
	if err != nil {
		t.Fatal("error requesting a sum", err)
	}
	if x := res.Data.(*sumResponse).X; x != 9+5 {
		t.Fatalf("math service is drunk, 9 + 5 != %d", x)
	}

	// do a subtraction
	res, err = letterbox.Request(ctx, mathSvcTopic, pubsub.Message{
		Name: subtractRequestEventName,
		Data: &subtractRequest{A: 2, B: 8},
	})
	if err != nil {
		t.Fatal("error requesting a subtraction", err)
	}
	if x := res.Data.(*subtractResponse).X; x != 2-8 {
		t.Fatalf("math service is drunk, 2 - 8 != %d", x)
	}

	cancel()

	select {
	case <-time.NewTimer(time.Second).C:
		t.Fatal("timeout waiting for a clean stop!")
	case <-consumersStopped:
	}
}
