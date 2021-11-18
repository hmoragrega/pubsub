package pubsub_test

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hmoragrega/pubsub"
	"github.com/hmoragrega/pubsub/internal/stubs"
)

var (
	errorDummy   = errors.New("dummy error")
	handlerDummy = pubsub.HandlerFunc(func(_ context.Context, _ *pubsub.Message) error {
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

	t.Run("cannot subscribe twice using the same consumer name", func(t *testing.T) {
		var router pubsub.Router

		err := router.Register("same", &stubs.SubscriberStub{}, handlerDummy)
		if err != nil {
			t.Fatalf("unexpected error registering the consumer: %v", err)
		}

		err = router.Register("same", &stubs.SubscriberStub{}, handlerDummy)
		if err == nil {
			t.Fatalf("unexpected success registering the consumer again: %v", err)
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
			if err := router.Register(strconv.Itoa(i), s, handlerDummy); err != nil {
				t.Fatalf("cannot register handler %d: %v", i, err)
			}
			s.SubscribeFunc = func() (<-chan pubsub.Next, error) {
				return nil, <-results
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
			if err := router.Register(strconv.Itoa(i), s, handlerDummy); err != nil {
				t.Fatalf("cannot register handler %d: %v", i, err)
			}
			s.SubscribeFunc = func() (<-chan pubsub.Next, error) {
				next := make(chan pubsub.Next, 1)
				if i == 2 {
					next <- pubsub.Next{Err: errorDummy}
				} else {
					next <- pubsub.Next{Err: context.Canceled}
				}
				return next, nil
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
			if err := router.Register(strconv.Itoa(i), s, handlerDummy); err != nil {
				t.Fatalf("cannot register handler %d: %v", i, err)
			}
			s.SubscribeFunc = func() (<-chan pubsub.Next, error) {
				running <- struct{}{}
				return nil, nil
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
		consumerName := "foo"

		var checkpointsCalled int

		ctx, cancel := context.WithCancel(context.Background())
		msg := stubs.NewNoOpReceivedMessage()
		msg.AckFunc = func(ctx context.Context) error {
			cancel()
			return nil
		}

		verifyCheckpoint := func(checkpoint string, name string, message pubsub.ReceivedMessage, err error) error {
			checkpointsCalled++
			if err != nil {
				return err
			}
			if message.(*stubs.ReceivedMessageStub) != msg {
				return fmt.Errorf("%s mesage is not the equal; got %+v", checkpoint, message)
			}
			if name != consumerName {
				return fmt.Errorf("%s consumer is not correct; got %+v, want %s", checkpoint, name, consumerName)
			}
			return nil
		}
		router := pubsub.Router{
			OnReceive: func(_ context.Context, name string, message pubsub.ReceivedMessage, err error) error {
				return verifyCheckpoint("OnReceive", name, message, err)
			},
			OnUnmarshal: func(_ context.Context, name string, message pubsub.ReceivedMessage, err error) error {
				return verifyCheckpoint("OnUnmarshal", name, message, err)
			},
			OnHandler: func(_ context.Context, name string, message pubsub.ReceivedMessage, err error) error {
				return verifyCheckpoint("OnHandler", name, message, err)
			},
			OnAck: func(_ context.Context, name string, message pubsub.ReceivedMessage, err error) error {
				return verifyCheckpoint("OnAck", name, message, err)
			},
		}

		s := &stubs.SubscriberStub{
			SubscribeFunc: func() (<-chan pubsub.Next, error) {
				next := make(chan pubsub.Next, 1)
				next <- pubsub.Next{Message: msg}

				return next, nil
			},
			StopFunc: func(ctx context.Context) error {
				return nil
			},
		}

		err := router.Register(consumerName, s, handlerDummy)
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

	t.Run("checkpoint exits", func(t *testing.T) {
		routers := []*pubsub.Router{
			{
				OnReceive: func(_ context.Context, _ string, _ pubsub.ReceivedMessage, _ error) error {
					return errorDummy
				},
			}, {
				OnUnmarshal: func(_ context.Context, _ string, _ pubsub.ReceivedMessage, _ error) error {
					return errorDummy
				},
			}, {
				OnHandler: func(_ context.Context, _ string, _ pubsub.ReceivedMessage, _ error) error {
					return errorDummy
				},
			}, {
				OnAck: func(_ context.Context, _ string, _ pubsub.ReceivedMessage, _ error) error {
					return errorDummy
				},
			},
		}
		for _, router := range routers {
			s := &stubs.SubscriberStub{
				SubscribeFunc: func() (<-chan pubsub.Next, error) {
					msg := stubs.NewNoOpReceivedMessage()
					msg.AckFunc = func(ctx context.Context) error {
						return nil
					}

					next := make(chan pubsub.Next, 1)
					next <- pubsub.Next{Message: msg}

					return next, nil
				},
				StopFunc: func(ctx context.Context) error {
					return nil
				},
			}
			err := router.Register("foo", s, handlerDummy)
			if err != nil {
				t.Fatal("cannot register handler", err)
			}

			err = router.Run(ctx)
			if !errors.Is(err, errorDummy) {
				t.Fatalf("expected checkpoint exit error; got %v", err)
			}
		}
	})

	t.Run("continue on errors", func(t *testing.T) {
		s := &stubs.SubscriberStub{
			SubscribeFunc: func() (<-chan pubsub.Next, error) {
				next := make(chan pubsub.Next, 5)
				next <- pubsub.Next{Err: errorDummy}
				next <- pubsub.Next{Message: stubs.NewNoOpReceivedMessage()}
				next <- pubsub.Next{Message: stubs.NewNoOpReceivedMessage()}
				next <- pubsub.Next{Message: stubs.NewNoOpReceivedMessage()}
				next <- pubsub.Next{Message: stubs.NewNoOpReceivedMessage()}
				return next, nil
			},
			StopFunc: func(ctx context.Context) error {
				return nil
			},
		}
		var (
			unmarshallerCalls int
			handlerCalls      int
		)
		router := pubsub.Router{
			Unmarshaller: pubsub.UnmarshallerFunc(func(topic string, msg pubsub.ReceivedMessage) (interface{}, error) {
				unmarshallerCalls++
				if unmarshallerCalls == 1 {
					return nil, errorDummy
				}
				return nil, nil
			}),
			AckDecider: pubsub.DisableAutoAck,
			MessageContext: func(parent context.Context, _ pubsub.ReceivedMessage) context.Context {
				return parent
			},
		}

		ctx, cancel := context.WithCancel(context.Background())
		err := router.Register("foo", s, pubsub.HandlerFunc(func(ctx context.Context, _ *pubsub.Message) error {
			handlerCalls++
			switch handlerCalls {
			case 1:
				return errorDummy
			case 2:
				return nil
			}
			cancel()
			return ctx.Err()
		}))
		if err != nil {
			t.Fatal("cannot register handler", err)
		}

		err = router.Run(ctx)
		if err != nil {
			t.Fatalf("expected clean shutdown; got %v", err)
		}
	})

	t.Run("errors on running routers", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var router pubsub.Router
		_ = router.Register("foo", &stubs.SubscriberStub{
			SubscribeFunc: func() (<-chan pubsub.Next, error) {
				return nil, nil
			},
			StopFunc: func(ctx context.Context) error {
				return nil
			},
		}, handlerDummy)

		errs := make(chan error, 2)
		go func() {
			errs <- router.Run(ctx)
		}()
		go func() {
			errs <- router.Run(ctx)
		}()

		err := <-errs

		if !errors.Is(err, pubsub.ErrRouterAlreadyRunning) {
			t.Fatalf("unexpected error starting the router twice; got %v", err)
		}
		err = router.Register("bar", &stubs.SubscriberStub{}, handlerDummy)
		if !errors.Is(err, pubsub.ErrRouterAlreadyRunning) {
			t.Fatalf("unexpected error registering handlers when router is running; got %v", err)
		}

		// stop the router.
		cancel()
		<-errs

		err = router.Register("bar", &stubs.SubscriberStub{}, handlerDummy)
		if !errors.Is(err, pubsub.ErrRouterAlreadyStopped) {
			t.Fatalf("unexpected error registering handlers when router has stopped; got %v", err)
		}
		err = router.Run(ctx)
		if !errors.Is(err, pubsub.ErrRouterAlreadyStopped) {
			t.Fatalf("unexpected error starting a stopped router; got %v", err)
		}
	})

	t.Run("re-schedule messages with backoff", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Hour) //TODO
		defer cancel()

		var rescheduled int32 = 0
		var want int32 = 2
		router := pubsub.Router{
			OnAck: func(ctx context.Context, topic string, msg pubsub.ReceivedMessage, err error) error {
				// stop the router on ack errors.
				if err != nil {
					return err
				}
				// stop after all messages have been rescheduled correctly.
				if want == atomic.AddInt32(&rescheduled, 1) {
					cancel()
				}
				return nil
			},
		}

		errHandler := pubsub.HandlerFunc(func(_ context.Context, _ *pubsub.Message) error {
			return errors.New("dummy err")
		})

		defaultChan := make(chan pubsub.Next)
		overrideChan := make(chan pubsub.Next)

		// normal consumer with default exponential backoff
		_ = router.Register("default", &stubs.SubscriberStub{
			SubscribeFunc: func() (<-chan pubsub.Next, error) {
				return defaultChan, nil
			},
			StopFunc: func(ctx context.Context) error {
				return nil
			},
		}, errHandler, pubsub.WithAckDecider(pubsub.ReScheduleOnError))

		// consumer with overridden linear backoff.
		_ = router.Register("override", &stubs.SubscriberStub{
			SubscribeFunc: func() (<-chan pubsub.Next, error) {
				return overrideChan, nil
			},
			StopFunc: func(ctx context.Context) error {
				return nil
			},
		}, errHandler, pubsub.WithAckDecider(pubsub.ReScheduleOnError), pubsub.WithBackoff(pubsub.LinearBackoff(time.Second)))

		go func() {
			m := stubs.NewNoOpReceivedMessage()
			m.ReScheduleFunc = func(ctx context.Context, delay time.Duration) error {
				if delay != time.Minute {
					return fmt.Errorf("unexpected re-scheduling delay in the default backoff")
				}
				return nil
			}
			defaultChan <- pubsub.Next{Message: m}
		}()

		go func() {
			m := stubs.NewNoOpReceivedMessage()
			m.ReScheduleFunc = func(ctx context.Context, delay time.Duration) error {
				if delay != time.Second {
					return fmt.Errorf("unexpected re-scheduling delay in the overridden backoff")
				}
				return nil
			}
			overrideChan <- pubsub.Next{Message: m}
		}()

		err := router.Run(ctx)
		if err != nil {
			t.Fatalf("unexpected router error: %v", err)
		}
		if err := ctx.Err(); err != context.Canceled {
			t.Fatalf("unexpected context error: %v", err)
		}
	})
}
