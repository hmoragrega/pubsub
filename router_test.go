package pubsub_test

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/hmoragrega/pubsub"
	"github.com/hmoragrega/pubsub/internal/stubs"
)

var (
	errorDummy   = errors.New("dummy error")
	handlerDummy = pubsub.MessageHandlerFunc(func(_ context.Context, _ *pubsub.Message) error {
		return nil
	})
)

func TestRouter_Run(t *testing.T) {
	ctx := context.Background()

	t.Run("zero value router can run without failure", func(t *testing.T) {
		var router pubsub.Router
		err := router.Run(ctx)
		if err != nil {
			t.Fatalf("unexpected error running an empty Router: %v", err)
		}
	})

	t.Run("cannot subscribe twice to the same topic", func(t *testing.T) {
		var router pubsub.Router

		err := router.RegisterHandler("same", &stubs.SubscriberStub{}, handlerDummy)
		if err != nil {
			t.Fatalf("unexpected error registering the topic: %v", err)
		}

		err = router.RegisterHandler("same", &stubs.SubscriberStub{}, handlerDummy)
		if err == nil {
			t.Fatalf("unexpected success registering the topic again: %v", err)
		}
	})

	t.Run("started consumers are stopped on any subscribe failure", func(t *testing.T) {
		// feed the results for the mocks, we cannot know
		// which will be the order due to the randomness
		// of iterating the consumer map.
		results := make(chan error, 3)
		results <- nil
		results <- nil
		results <- errorDummy

		var (
			router  pubsub.Router
			stopped uint32
		)
		for i, s := range [3]*stubs.SubscriberStub{{}, {}, {}} {
			if err := router.RegisterHandler(strconv.Itoa(i), s, handlerDummy); err != nil {
				t.Fatalf("cannot register handler %d: %v", i, err)
			}
			s.SubscribeFunc = func() error {
				return <-results
			}
			s.StopFunc = func(_ context.Context) error {
				atomic.AddUint32(&stopped, 1)
				return nil
			}
		}

		err := router.Run(ctx)

		if !errors.Is(err, errorDummy) {
			t.Fatalf("unexpected error: got %v, want %v", err, errorDummy)
		}
		if stopped != 2 {
			t.Fatalf("unexpected number of stopped subscribers: got %d, want 2", stopped)
		}
	})

	t.Run("all consumers are stopped by default if one fails", func(t *testing.T) {
		router := pubsub.Router{
			OnReceive: func(_ context.Context, _ string, _ pubsub.ReceivedMessage, err error) error {
				return err
			},
		}
		var stopped uint32
		for i, s := range [3]*stubs.SubscriberStub{{}, {}, {}} {
			i := i
			if err := router.RegisterHandler(strconv.Itoa(i), s, handlerDummy); err != nil {
				t.Fatalf("cannot register handler %d: %v", i, err)
			}
			s.SubscribeFunc = func() error {
				return nil
			}
			s.NextFunc = func(ctx context.Context) (pubsub.ReceivedMessage, error) {
				if i == 2 {
					return nil, errorDummy
				}
				return nil, context.Canceled
			}
			s.StopFunc = func(_ context.Context) error {
				atomic.AddUint32(&stopped, 1)
				return nil
			}
		}

		err := router.Run(ctx)

		if !errors.Is(err, errorDummy) {
			t.Fatalf("unexpected error: got %v, want %v", err, errorDummy)
		}
		if stopped != 3 {
			t.Fatalf("unexpected number of stopped subscribers: got %d, want 2", stopped)
		}
	})

	t.Run("errors stopping the consumers are reported", func(t *testing.T) {
		var (
			router    pubsub.Router
			consumers uint32 = 3
		)
		results := make(chan error, consumers)
		results <- nil
		results <- nil
		results <- errorDummy

		ctx, cancel := context.WithCancel(ctx)
		running := make(chan struct{}, consumers)

		for i, s := range [3]*stubs.SubscriberStub{{}, {}, {}} {
			if err := router.RegisterHandler(strconv.Itoa(i), s, handlerDummy); err != nil {
				t.Fatalf("cannot register handler %d: %v", i, err)
			}
			s.SubscribeFunc = func() error {
				return nil
			}
			s.NextFunc = func(ctx context.Context) (pubsub.ReceivedMessage, error) {
				running <- struct{}{}
				<-ctx.Done()
				return nil, ctx.Err()
			}
			s.StopFunc = func(_ context.Context) error {
				return <-results
			}
		}

		go func() {
			// wait until all consumers are running
			for i := 0; i < int(consumers); i++ {
				<-running
			}
			cancel()
		}()

		err := router.Run(ctx)

		if !errors.Is(err, errorDummy) {
			t.Fatalf("unexpected error: got %v, want %v", err, errorDummy)
		}
	})

	t.Run("checkpoints are called", func(t *testing.T) {
		subscriberTopic := "foo"

		var checkpointsCalled int
		msg := stubs.ReceivedMessageStub{
			AckFunc: func(ctx context.Context) error {
				return nil
			},
		}
		verifyCheckpoint := func(checkpoint string, topic string, message pubsub.ReceivedMessage, err error) error {
			checkpointsCalled++
			if err != nil {
				return err
			}
			if message.(*stubs.ReceivedMessageStub) != &msg {
				return fmt.Errorf("%s mesage is not the equal; got %+v", checkpoint, message)
			}
			if topic != subscriberTopic {
				return fmt.Errorf("%s topic is not correct; got %+v, want %s", checkpoint, topic, subscriberTopic)
			}
			return nil
		}
		router := pubsub.Router{
			Unmarshaler: pubsub.UnmarshalerFunc(func(message pubsub.ReceivedMessage) (*pubsub.Message, error) {
				return nil, nil
			}),
			OnReceive: func(_ context.Context, topic string, message pubsub.ReceivedMessage, err error) error {
				return verifyCheckpoint("OnReceive", topic, message, err)
			},
			OnUnmarshal: func(_ context.Context, topic string, message pubsub.ReceivedMessage, err error) error {
				return verifyCheckpoint("OnUnmarshal", topic, message, err)
			},
			OnHandler: func(_ context.Context, topic string, message pubsub.ReceivedMessage, err error) error {
				return verifyCheckpoint("OnHandler", topic, message, err)
			},
			OnAck: func(_ context.Context, topic string, message pubsub.ReceivedMessage, err error) error {
				return verifyCheckpoint("OnAck", topic, message, err)
			},
		}

		var count int
		s := &stubs.SubscriberStub{
			SubscribeFunc: func() error {
				return nil
			},
			NextFunc: func(ctx context.Context) (pubsub.ReceivedMessage, error) {
				// return the message stub on the first iteration
				// and stop the consumer in the next one.
				count++
				if count == 1 {
					return &msg, nil
				}
				return nil, context.Canceled
			},
			StopFunc: func(ctx context.Context) error {
				return nil
			},
		}

		err := router.RegisterHandler(subscriberTopic, s, handlerDummy)
		if err != nil {
			t.Fatal("cannot register handler", err)
		}

		err = router.Run(ctx)
		if err != nil {
			t.Fatal("unexpected error in the router", err)
		}
		if checkpointsCalled != 4 {
			t.Fatalf("unexpected number of hooks called; got %d, want 4", checkpointsCalled)
		}
	})
}
