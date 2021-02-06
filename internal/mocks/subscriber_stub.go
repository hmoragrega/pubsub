package mocks

import (
	"context"

	"github.com/hmoragrega/pubsub"
)

type SubscriberStub struct {
	SubscribeFunc func() error
	NextFunc      func(ctx context.Context) (pubsub.ReceivedMessage, error)
	StopFunc      func(ctx context.Context) error
}

func (s *SubscriberStub) Subscribe() error {
	return s.SubscribeFunc()
}

func (s *SubscriberStub) Next(ctx context.Context) (pubsub.ReceivedMessage, error) {
	return s.NextFunc(ctx)
}

func (s *SubscriberStub) Stop(ctx context.Context) error {
	return s.StopFunc(ctx)
}
