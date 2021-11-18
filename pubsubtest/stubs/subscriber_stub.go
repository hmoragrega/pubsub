package stubs

import (
	"context"

	"github.com/hmoragrega/pubsub"
)

type SubscriberStub struct {
	SubscribeFunc func() (<-chan pubsub.Next, error)
	StopFunc      func(ctx context.Context) error
}

func (s *SubscriberStub) Subscribe() (<-chan pubsub.Next, error) {
	return s.SubscribeFunc()
}

func (s *SubscriberStub) Stop(ctx context.Context) error {
	return s.StopFunc(ctx)
}
