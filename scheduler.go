package pubsub

import (
	"context"
	"fmt"
	"time"
)

type Scheduler interface {
	Publisher
	Schedule(ctx context.Context, dueDate time.Time, topic string, messages ...*Message) error
	Delay(ctx context.Context, delay time.Duration, topic string, messages ...*Message) error
}

type DueMessage struct {
	Topic    string
	Envelope *Envelope
	Err      error
}

type SchedulerStorage interface {
	// Schedule schedules a message to be published in the future.
	Schedule(ctx context.Context, dueDate time.Time, topic string, messages ...*Envelope) error

	// ConsumeDue returns a channel that should receive the next due messages or an error.
	// It should feed the next messages while the context is valid, once the context terminates
	// it should make any cleaning task and close the channel to signal a proper cleanup.
	ConsumeDue(ctx context.Context) (<-chan DueMessage, error)

	// Published indicates to the storage that the message has been published.
	Published(ctx context.Context, message DueMessage) error
}

var _ Scheduler = &SchedulerPublisher{}

type SchedulerPublisher struct {
	*MarshallerPublisher
	storage SchedulerStorage
	now     func() time.Time
}

func NewSchedulerPublisher(pub *MarshallerPublisher, storage SchedulerStorage) *SchedulerPublisher {
	return &SchedulerPublisher{
		MarshallerPublisher: pub,
		storage:             storage,
		now:                 time.Now,
	}
}

func (p *SchedulerPublisher) Delay(ctx context.Context, duration time.Duration, topic string, messages ...*Message) error {
	return p.Schedule(ctx, p.now().Add(duration), topic, messages...)
}

func (p *SchedulerPublisher) Schedule(ctx context.Context, dueDate time.Time, topic string, messages ...*Message) error {
	envelopes, err := marshallMessages(p.marshaller, messages...)
	if err != nil {
		return err
	}

	return p.storage.Schedule(ctx, dueDate, topic, envelopes...)
}

func (p *SchedulerPublisher) PublishDue(ctx context.Context) error {
	feed, err := p.storage.ConsumeDue(ctx)
	if err != nil {
		return fmt.Errorf("cannot start consuming due messages: %w", err)
	}

	for next := range feed {
		if err := next.Err; err != nil {
			return fmt.Errorf("cannot receive next due message: %w", err)
		}
		if err := p.publisher.Publish(ctx, next.Topic, next.Envelope); err != nil {
			return fmt.Errorf("cannot publish due message: %w", err)
		}
		if err := p.storage.Published(ctx, next); err != nil {
			return fmt.Errorf("cannot notify message publication: %w", err)
		}
	}

	return nil
}
