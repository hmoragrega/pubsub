package pubsub_test

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/hmoragrega/pubsub"
	"github.com/hmoragrega/pubsub/internal/stubs"
	"github.com/hmoragrega/pubsub/marshaller"
)

func TestPublisher_PublishMarshallFailure(t *testing.T) {
	fakeErr := errors.New("foo")
	m := &stubs.MarshallerStub{
		MarshalFunc: func(data interface{}) ([]byte, string, error) {
			if data.(string) != "data" {
				t.Fatalf("unexpected data to marshall; got %+v", data)
			}
			return nil, "", fakeErr
		},
	}

	p := pubsub.NewPublisher(nil, m)

	err := p.Publish(context.Background(), "foo", &pubsub.Message{Data: "data"})
	if !errors.Is(err, fakeErr) {
		t.Fatalf("unexpected error result; got %v", err)
	}
}

func TestPublisher_PublishHandlerSendMessages(t *testing.T) {
	var (
		publishedMessages []*pubsub.Envelope
		publishedTopic    string
		dummyErr          = errors.New("some error")
	)
	p := pubsub.NewPublisher(pubsub.EnvelopePublisherFunc(func(ctx context.Context, topic string, envelopes ...*pubsub.Envelope) error {
		publishedTopic = topic
		publishedMessages = envelopes
		return dummyErr
	}), &marshaller.ByteMarshaller{})

	topicToSend := "foo"
	messagesToSend := []*pubsub.Message{
		{Data: "foo"},
		{Data: "bar"},
	}

	err := p.Handler(topicToSend, pubsub.PublisherHandlerFunc(func(ctx context.Context, message *pubsub.Message) ([]*pubsub.Message, error) {
		return messagesToSend, nil
	})).HandleMessage(context.Background(), &pubsub.Message{})

	if !errors.Is(err, dummyErr) {
		t.Fatalf("expected error; got %v, want %v", err, dummyErr)
	}
	if publishedTopic != topicToSend {
		t.Fatalf("unpected pubslihing topic; got %v, want %v", publishedTopic, topicToSend)
	}
	if reflect.DeepEqual(messagesToSend, publishedMessages) {
		t.Fatalf("expected error; got %v, want %v", err, dummyErr)
	}
}

func TestPublisher_PublishHandlerDoNotSend(t *testing.T) {
	p := pubsub.NewPublisher(pubsub.EnvelopePublisherFunc(func(ctx context.Context, topic string, envelopes ...*pubsub.Envelope) error {
		t.Error("publisher call was not expected")
		return nil
	}), &marshaller.ByteMarshaller{})

	dummyErr := errors.New("some error")

	err := p.Handler("foo", pubsub.PublisherHandlerFunc(func(ctx context.Context, message *pubsub.Message) ([]*pubsub.Message, error) {
		return nil, dummyErr
	})).HandleMessage(context.Background(), &pubsub.Message{})

	if !errors.Is(err, dummyErr) {
		t.Fatalf("expected error; got %v, want %v", err, dummyErr)
	}
}

func TestPublisher_Wrapper(t *testing.T) {
	injectedValue := "some-value"
	attributeKey := "some-key"
	ctxKey := "some-ctx-key"

	innerPublisher := pubsub.PublisherFunc(func(ctx context.Context, topic string, envelopes ...*pubsub.Message) error {
		for _, e := range envelopes {
			if e.Attributes[attributeKey] != injectedValue {
				t.Fatalf("message does not have the injected attribute; got %+v", e.Attributes)
			}
			if e.Name != topic {
				t.Fatalf("topic should be used as event name; got %+v", e.Name)
			}
		}
		return nil
	})

	p := pubsub.WrapPublisher(innerPublisher,
		pubsub.TopicAsEventName(),
		pubsub.CtxAttributeInjector(attributeKey, func(ctx context.Context) string {
			return ctx.Value(ctxKey).(string)
		}),
	)

	ctx := context.WithValue(context.Background(), ctxKey, injectedValue)
	err := p.Publish(ctx, "foo-topic", &pubsub.Message{})
	if err != nil {
		t.Fatal("unexpected error publishing", err)
	}
}
