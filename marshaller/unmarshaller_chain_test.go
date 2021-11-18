package marshaller

import (
	"errors"
	"reflect"
	"testing"

	"github.com/hmoragrega/pubsub"
	"github.com/hmoragrega/pubsub/pubsubtest/stubs"
)

func TestChainUnmarshaller_Success(t *testing.T) {
	msg := &pubsub.Message{ID: "foo"}
	c := NewChainUnmarshaller(
		pubsub.UnmarshallerFunc(func(topic string, message pubsub.ReceivedMessage) (interface{}, error) {
			return nil, errors.New("some error")
		}),
		pubsub.UnmarshallerFunc(func(topic string, message pubsub.ReceivedMessage) (interface{}, error) {
			return msg, nil
		}),
	)
	got, err := c.Unmarshal("foo", &stubs.ReceivedMessageStub{})
	if err != nil {
		t.Fatal("unexpected error", err)
	}
	if !reflect.DeepEqual(msg, got) {
		t.Fatalf("unexpected message; got %+v, want %+v", got, msg)
	}
}

func TestChainUnmarshaller_Failure(t *testing.T) {
	dummyError := errors.New("some error")
	one := make(chan struct{}, 1)
	two := make(chan struct{}, 1)
	c := NewChainUnmarshaller(
		pubsub.UnmarshallerFunc(func(topic string, message pubsub.ReceivedMessage) (interface{}, error) {
			one <- struct{}{}
			return nil, dummyError
		}),
		pubsub.UnmarshallerFunc(func(topic string, message pubsub.ReceivedMessage) (interface{}, error) {
			two <- struct{}{}
			return nil, dummyError
		}),
	)
	result := make(chan error, 1)
	go func() {
		_, err := c.Unmarshal("foo", &stubs.ReceivedMessageStub{})
		result <- err
	}()

	<-one
	<-two
	err := <-result

	if !errors.Is(err, dummyError) {
		t.Fatal("unexpected error", err)
	}
}
