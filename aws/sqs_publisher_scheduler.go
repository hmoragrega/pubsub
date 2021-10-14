package aws

import (
	"context"
	"time"

	"github.com/hmoragrega/pubsub"
)

var (
	_ pubsub.SchedulerStorage  = &SQSSchedulerStorage{}
	_ pubsub.EnvelopePublisher = &SQSSchedulerStorage{}
)

// SQSSchedulerStorage a publisher that can publish directly to queues.
type SQSSchedulerStorage struct {
	*SQSPublisher
	pubsub.SchedulerStorage
}

// NewSQSSchedulerStorage creates a new hybrid SQS publisher + scheduler storage
func NewSQSSchedulerStorage(pub *SQSPublisher, storage pubsub.SchedulerStorage) *SQSSchedulerStorage {
	return &SQSSchedulerStorage{
		SchedulerStorage: storage,
		SQSPublisher:     pub,
	}
}

// Schedule schedules a message to be published in the future.
func (s *SQSSchedulerStorage) Schedule(ctx context.Context, dueDate time.Time, queue string, messages ...*pubsub.Envelope) error {
	delay := dueDate.Sub(time.Now()).Round(time.Second)

	// SQS accepts delays up to 15 minutes with seconds precision.
	if delay <= 15*time.Minute {
		return s.publishWithDelay(ctx, queue, int32(delay.Seconds()), messages...)
	}

	return s.SchedulerStorage.Schedule(ctx, dueDate, queue, messages...)
}