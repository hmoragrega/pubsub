package letterbox

import (
	"context"
	"errors"
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

func TestLetterbox_RequestFailures(t *testing.T) {
	var ctx = context.Background()
	var letterbox Letterbox

	t.Run("nil request", func(t *testing.T) {
		if _, err := letterbox.Request(ctx, "foo", nil); err == nil {
			t.Fatal("expected error, got", err)
		}
	})
	t.Run("duplicated request ID", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())

		letterbox.Publisher = pubsub.PublisherFunc(func(_ context.Context, _ string, _ ...*pubsub.Message) error {
			return nil
		})
		msg := &pubsub.Message{
			ID: "123",
		}
		errs := make(chan error, 2)
		go func() {
			_, err := letterbox.Request(ctx, "foo", msg)
			errs <- err
		}()
		go func() {
			_, err := letterbox.Request(ctx, "foo", msg)
			errs <- err
		}()

		err := <- errs
		if !errors.Is(err, ErrRequestAlreadySent) {
			t.Fatal("expected error, got", err)
		}

		if _, ok := letterbox.pop("123"); !ok {
			t.Fatal("the first message should be there")
		}
		cancel()
	})
	t.Run("timeout ", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		letterbox.Publisher = pubsub.PublisherFunc(func(_ context.Context, _ string, _ ...*pubsub.Message) error {
			cancel()
			return nil
		})

		_, err := letterbox.Request(ctx, "foo", &pubsub.Message{
				ID: "123",
		})
		if !errors.Is(err, context.Canceled) {
			t.Fatal("expected timeout, got", err)
		}
	})
	t.Run("publisher error", func(t *testing.T) {
		var dummyErr = errors.New("some error")
		letterbox.Publisher = pubsub.PublisherFunc(func(_ context.Context, _ string, _ ...*pubsub.Message) error {
			return dummyErr
		})
		if _, err := letterbox.Request(ctx, "foo", &pubsub.Message{}); !errors.Is(err, dummyErr) {
			t.Fatalf("expected error %v, got %v", dummyErr, err)
		}
	})
}

func TestLetterbox_ResponseFailures(t *testing.T) {
	var ctx = context.Background()
	var letterbox Letterbox

	t.Run("nil request", func(t *testing.T) {
		if err := letterbox.Response(ctx, nil, nil); err == nil {
			t.Fatal("expected error, got", err)
		}
	})
	t.Run("nil response", func(t *testing.T) {
		if err := letterbox.Response(ctx, &pubsub.Message{}, nil); err == nil {
			t.Fatal("expected error, got", err)
		}
	})
	t.Run("missing response topic", func(t *testing.T) {
		if err := letterbox.Response(ctx, &pubsub.Message{}, &pubsub.Message{}); err == nil {
			t.Fatal("expected error, got", err)
		}
	})
	t.Run("missing request ID", func(t *testing.T) {
		if err := letterbox.Response(ctx, &pubsub.Message{
			Attributes: map[string]string{
				responseTopicAttribute: "foo-topic",
			},
		}, &pubsub.Message{}); err == nil {
			t.Fatal("expected error, got", err)
		}
	})
	t.Run("missing requested at", func(t *testing.T) {
		if err := letterbox.Response(ctx, &pubsub.Message{
			Attributes: map[string]string{
				responseTopicAttribute: "foo-topic",
				requestIDAttribute:     "123",
			},
		}, &pubsub.Message{}); err == nil {
			t.Fatal("expected error, got", err)
		}
	})
	t.Run("publisher error", func(t *testing.T) {
		var dummyErr = errors.New("some error")
		letterbox.Publisher = pubsub.PublisherFunc(func(ctx context.Context, topic string, envelopes ...*pubsub.Message) error {
			return dummyErr
		})
		err := letterbox.Response(ctx, &pubsub.Message{
			Attributes: map[string]string{
				responseTopicAttribute: "foo-topic",
				requestIDAttribute:     "123",
				requestedAtAttribute:   time.Now().Format(time.RFC3339Nano),
			},
		}, &pubsub.Message{})
		if !errors.Is(err, dummyErr) {
			t.Fatal("unexpected error, got", err)
		}
	})
}

func TestLetterbox_HandleMessageFailures(t *testing.T) {
	var ctx = context.Background()
	var letterbox Letterbox

	t.Run("nil response", func(t *testing.T) {
		if err := letterbox.HandleMessage(ctx, nil); err == nil {
			t.Fatal("expected error, got", err)
		}
	})
	t.Run("missing request ID", func(t *testing.T) {
		if err := letterbox.HandleMessage(ctx, &pubsub.Message{}); err == nil {
			t.Fatal("expected error, got", err)
		}
	})
	t.Run("missing requested at", func(t *testing.T) {
		if err := letterbox.HandleMessage(ctx, &pubsub.Message{
			Attributes: map[string]string{
				requestIDAttribute: "123",
			},
		}); err == nil {
			t.Fatal("expected error, got", err)
		}
	})
	t.Run("invalid requested at", func(t *testing.T) {
		if err := letterbox.HandleMessage(ctx, &pubsub.Message{
			Attributes: map[string]string{
				requestIDAttribute:   "123",
				requestedAtAttribute: "bad",
			},
		}); err == nil {
			t.Fatal("expected error, got", err)
		}
	})
	t.Run("miss request does not return errors", func(t *testing.T) {
		if err := letterbox.HandleMessage(ctx, &pubsub.Message{
			Attributes: map[string]string{
				requestIDAttribute:   "123",
				requestedAtAttribute: time.Now().Format(time.RFC3339Nano),
			},
		}); err != nil {
			t.Fatal("unexpected err, got", err)
		}
	})
}
