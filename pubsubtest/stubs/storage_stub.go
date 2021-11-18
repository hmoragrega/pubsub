package stubs

import (
	"context"
	"time"

	"github.com/hmoragrega/pubsub"
)

type StorageStub struct {
	ScheduleFunc   func(ctx context.Context, dueDate time.Time, topic string, messages ...*pubsub.Envelope) error
	ConsumeDueFunc func(ctx context.Context) (<-chan pubsub.DueMessage, error)
	PublishedFunc  func(ctx context.Context, message pubsub.DueMessage) error
}

func (m StorageStub) Schedule(ctx context.Context, dueDate time.Time, topic string, messages ...*pubsub.Envelope) error {
	return m.ScheduleFunc(ctx, dueDate, topic, messages...)
}

func (m StorageStub) ConsumeDue(ctx context.Context) (<-chan pubsub.DueMessage, error) {
	return m.ConsumeDueFunc(ctx)
}

func (m StorageStub) Published(ctx context.Context, message pubsub.DueMessage) error {
	return m.PublishedFunc(ctx, message)
}
