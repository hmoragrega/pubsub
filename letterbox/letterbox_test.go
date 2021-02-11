package letterbox

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/hmoragrega/pubsub"
	"github.com/hmoragrega/pubsub/internal/channels"
	"github.com/hmoragrega/pubsub/marshaller"
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

func TestLetterbox(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	var (
		instanceTopic  = fmt.Sprintf("letterbox-instance")
		mathSvcTopic   = fmt.Sprintf("letterbox-math-svc")
		jsonMarshaller marshaller.JSONMarshaller
	)

	var (
		sumRequestEventName       = "sum-request"
		sumResponseEventName      = "sum-response"
		subtractRequestEventName  = "subtract-request"
		subtractResponseEventName = "subtract-response"
	)

	_ = jsonMarshaller.Register(sumRequestEventName, &sumRequest{})
	_ = jsonMarshaller.Register(sumResponseEventName, &sumResponse{})
	_ = jsonMarshaller.Register(subtractRequestEventName, &subtractRequest{})
	_ = jsonMarshaller.Register(subtractResponseEventName, &subtractResponse{})

	channelsPub := channels.Publisher{}
	publisher := pubsub.NewPublisher(&channelsPub, &jsonMarshaller)

	letterbox := Letterbox{
		Publisher: publisher,
		Topic:     instanceTopic,
	}

	router := pubsub.Router{
		Unmarshaller: &jsonMarshaller,
	}

	err := router.Register(
		instanceTopic,
		channelsPub.Subscriber(instanceTopic),
		pubsub.Dispatcher(map[string]pubsub.Handler{
			sumResponseEventName:      &letterbox,
			subtractResponseEventName: &letterbox,
		}),
	)
	if err != nil {
		t.Fatal("cannot register instance subscriber", err)
	}

	err = router.Register(
		mathSvcTopic,
		channelsPub.Subscriber(mathSvcTopic),
		pubsub.Dispatcher(map[string]pubsub.Handler{
			sumRequestEventName: letterbox.Handler(
				func(ctx context.Context, request *pubsub.Message) (*pubsub.Message, error) {
					req := request.Data.(*sumRequest)
					return &pubsub.Message{
						Name: sumResponseEventName,
						Data: &sumResponse{X: req.A + req.B},
					}, nil
				}),
			subtractRequestEventName: letterbox.Handler(
				func(ctx context.Context, request *pubsub.Message) (*pubsub.Message, error) {
					req := request.Data.(*subtractRequest)
					return &pubsub.Message{
						Name: subtractResponseEventName,
						Data: &subtractResponse{X: req.A - req.B},
					}, nil
				}),
		}),
	)
	if err != nil {
		t.Fatal("cannot register math service subscriber", err)
	}

	routerStopped := make(chan error)
	go func() {
		routerStopped <- router.Run(ctx)
	}()

	// do a sum
	res, err := letterbox.Request(ctx, mathSvcTopic, &pubsub.Message{
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
	res, err = letterbox.Request(ctx, mathSvcTopic, &pubsub.Message{
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
	case err := <-routerStopped:
		if err != nil {
			t.Fatal("router stopped with an error!", err)
		}
	}
}

func TestLetterbox_Failures(t *testing.T) {

}
