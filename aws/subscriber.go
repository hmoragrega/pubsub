package aws

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/hmoragrega/pubsub"
)

type status uint8

const (
	MaxChangeVisibilityDelay = 12 * time.Hour

	started status = iota + 1
	stopped
)

var (
	ErrSubscriberStopped = errors.New("subscriber stopped")
	ErrAcknowledgement   = errors.New("cannot ack message")
	ErrNAcknowledgement  = errors.New("cannot nack message")
	ErrChangeVisibility  = errors.New("cannot change message visibility")
	ErrReSchedule        = errors.New("cannot re-schedule message")
	ErrAlreadyStarted    = errors.New("already started")
	ErrAlreadyStopped    = errors.New("already stopped")
	ErrMissingConfig     = errors.New("missing configuration")

	maxNumberOfMessages  = 10
	maxWaitTimeSeconds   = 20
	maxVisibilityTimeout = 12 * 60 * 60 // 12 hours
	allAttributes        = []string{"All"}
)

var _ pubsub.Subscriber = (*Subscriber)(nil)

type ackStrategy interface {
	Ack(ctx context.Context, msg *message) error
	NAck(ctx context.Context, msg *message) error
	Close(ctx context.Context) error
}

// AckConfig configures the acknowledgements behaviour.
type AckConfig struct {
	// Async will ack on the message asynchronously returning
	// immediately with success.
	//
	// Errors will be reported in the next consuming cycle.
	//
	// When the subscriber closes, it will wait until all
	// acknowledge operations finish, reporting any errors.
	Async bool

	// BatchSize will indicate to buffer acknowledgements
	// until a certain amount of messages are pending.
	//
	// Batching acknowledgements creates
	//
	// Calling Ack on the message will return success, and
	// the errors will be reported when consuming new messages
	//
	// When the subscriber closes, it will wait until all
	// acknowledge operation finish.
	BatchSize int

	// ChangeVisibilityOnNack when true, the message visibility
	// will be reset to zero so the message is redelivered again
	// immediately. It doesn't support batching.
	ChangeVisibilityOnNack bool

	// FlushEvery indicates how often the messages should be
	// acknowledged even if the batch is not full yet.
	//
	// This value has no effect if Batch is not true.
	FlushEvery time.Duration
}

// Subscriber for AWS SQS.
type Subscriber struct {
	// SQS Service.
	sqs sqsSvc

	// QueueURL for the SQS queue.
	queueURL string

	// AckConfig configuration the acknowledgements behaviour
	ackConfig AckConfig

	// Maximum number of messages per batch retrieve. Default 10.
	maxMessages int

	// Maximum time to wait while poling for new messages. Default 20s.
	waitTime *int

	// ackWaitTime indicates how much time the subscriber should wait
	// for all the messages in the batch to be acknowledged before requesting
	// a new batch.
	// Ideally this time should be greater than the message visibility, either
	// the specific for this subscriber or the queue default.
	// If not provided 30s will be used.
	ackWaitTime time.Duration

	// The duration (in seconds) that the received messages are hidden
	// from subsequent retrieve requests after being retrieved.
	visibilityTimeout int

	// Passing an optional storage will allow simulating changing the visibility
	// of a message above the SQS maximum of 15 minutes, in reality the messages
	// will be re-scheduled for publishing in the storage, and the original one
	// will be deleted from the queue.
	// Since both operations cannot be done translationally, it could lead to
	// duplicate messages if the message cannot be deleted from the queue.
	storage pubsub.SchedulerStorage

	// When re-scheduling a message above this threshold, the storage
	// will be used. SQS Max visibility will be used otherwise (12 hours).
	storageThreshold time.Duration

	next        chan pubsub.Next
	ackStrategy ackStrategy
	stopped     chan struct{}
	cancel      func()
	status      status
	statusMx    sync.RWMutex
}

// WithAck configures the acknowledgements behaviour
func WithAck(cfg AckConfig) func(s *Subscriber) {
	return func(s *Subscriber) {
		s.ackConfig = cfg
	}
}

// WithMaxMessages configures the number of messages to retrieve per request.
// If max messages <= 0 or > 10 the default will be used (10 messages).
func WithMaxMessages(maxMessages int) func(s *Subscriber) {
	return func(s *Subscriber) {
		s.maxMessages = maxMessages
	}
}

// WithWaitTime configures the time to wait during long poling waiting
// for new messages in the queue until the request is cancelled.
func WithWaitTime(waitTime int) func(s *Subscriber) {
	return func(s *Subscriber) {
		s.waitTime = &waitTime
	}
}

// WithVisibilityTimeout configures the time that the retrieved messages
// will be hidden from subsequent retrieve requests.
// If visibilityTimeout <= 0 the queue's default will be used.
// If it's greater than the 12 hours maximum, the maximum will be used: 43200s.
func WithVisibilityTimeout(visibilityTimeout int) func(s *Subscriber) {
	return func(s *Subscriber) {
		s.visibilityTimeout = visibilityTimeout
	}
}

// WithAckWaitTime indicates how much time the subscriber should wait
// for all the messages in the batch to be acknowledged before requesting
// a new batch.
// Ideally this time should be greater than the message visibility, either
// the specific for this subscriber or the queue default.
func WithAckWaitTime(ackWaitTime time.Duration) func(s *Subscriber) {
	return func(s *Subscriber) {
		s.ackWaitTime = ackWaitTime
	}
}

// WithStorage sets an optional storage that can be used to re-schedule
// the message beyond the maximum message visibility in SQS (15 minutes)
func WithStorage(storage pubsub.SchedulerStorage) func(s *Subscriber) {
	return func(s *Subscriber) {
		s.storage = storage
	}
}

// WithStorageThreshold sets the threshold above which the storage will
// be used when changing the message visibility.
func WithStorageThreshold(threshold time.Duration) func(s *Subscriber) {
	return func(s *Subscriber) {
		s.storageThreshold = threshold
	}
}

// NewSQSSubscriber creates a new SQS subscriber.
func NewSQSSubscriber(sqs sqsSvc, queueURL string, opts ...func(s *Subscriber)) *Subscriber {
	s := &Subscriber{
		sqs:              sqs,
		queueURL:         queueURL,
		storageThreshold: MaxChangeVisibilityDelay,
	}
	for _, o := range opts {
		o(s)
	}
	return s
}

// Subscribe subscribes to a SQS queue.
func (s *Subscriber) Subscribe() (<-chan pubsub.Next, error) {
	s.statusMx.Lock()
	defer s.statusMx.Unlock()

	if s.status >= started {
		return nil, ErrAlreadyStarted
	}
	if s.sqs == nil {
		return nil, fmt.Errorf("%w: SQS service not set", ErrMissingConfig)
	}
	if s.queueURL == "" {
		return nil, fmt.Errorf("%w: QueueURL cannot be empty", ErrMissingConfig)
	}
	if s.maxMessages <= 0 || s.maxMessages > maxNumberOfMessages {
		s.maxMessages = maxNumberOfMessages
	}
	if s.waitTime == nil {
		s.waitTime = &maxWaitTimeSeconds
	}
	if *s.waitTime < 0 || *s.waitTime > maxWaitTimeSeconds {
		s.waitTime = &maxWaitTimeSeconds
	}
	if s.visibilityTimeout < 0 {
		s.visibilityTimeout = 0
	}
	if s.visibilityTimeout > maxVisibilityTimeout {
		s.visibilityTimeout = maxVisibilityTimeout
	}
	if s.ackWaitTime == 0 {
		s.ackWaitTime = 30 * time.Second
	}

	if s.ackConfig.Async || s.ackConfig.BatchSize > 0 {
		if s.ackConfig.ChangeVisibilityOnNack {
			return nil, ErrAsyncNAckNotSupported
		}
		s.ackStrategy = newAsyncAck(s.sqs, s.queueURL, s.ackConfig)
	} else {
		s.ackStrategy = newSyncAck(s.sqs, s.queueURL, s.ackConfig.ChangeVisibilityOnNack)
	}

	ctx, cancel := context.WithCancel(context.Background())
	s.cancel = cancel
	s.status = started

	s.next = make(chan pubsub.Next, maxNumberOfMessages)
	s.stopped = make(chan struct{})

	go s.consume(ctx)

	return s.next, nil
}

// Stop stops consuming messages.
func (s *Subscriber) Stop(ctx context.Context) (err error) {
	s.statusMx.Lock()
	defer s.statusMx.Unlock()

	if s.status == stopped {
		return ErrAlreadyStopped
	}

	s.status = stopped
	s.cancel()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s.stopped:
	}

	return s.ackStrategy.Close(ctx)
}

// consumes the next batch of messages in
// the queue and puts them in the messages channel.
func (s *Subscriber) consume(ctx context.Context) {
	defer close(s.stopped)

	in := &sqs.ReceiveMessageInput{
		QueueUrl:              &s.queueURL,
		MaxNumberOfMessages:   int32(s.maxMessages),
		WaitTimeSeconds:       int32(*s.waitTime),
		VisibilityTimeout:     int32(s.visibilityTimeout),
		AttributeNames:        []types.QueueAttributeName{"All"},
		MessageAttributeNames: allAttributes,
	}

	t := time.NewTimer(s.ackWaitTime)
	defer func() {
		t.Stop()
	}()

	for {
		out, err := s.sqs.ReceiveMessage(ctx, in)
		if err != nil {
			select {
			case <-ctx.Done():
				return
			case s.next <- pubsub.Next{Err: err}:
				continue
			}
		}

		pending := len(out.Messages)
		if pending == 0 {
			continue
		}

		ackNotifications := make(chan struct{}, pending)

		messages, err := s.wrapMessages(out.Messages, ackNotifications)
		if err != nil {
			select {
			case <-ctx.Done():
				return
			case s.next <- pubsub.Next{Err: err}:
				continue
			}
		}

		for _, message := range messages {
			select {
			case <-ctx.Done():
				return
			case s.next <- pubsub.Next{Message: message}:
				continue
			}
		}

		t.Reset(s.ackWaitTime)
		for pending > 0 {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				continue
			case <-ackNotifications:
				pending--
			}
		}
	}
}

func (s *Subscriber) wrapMessages(in []types.Message, ackNotifications chan<- struct{}) (out []pubsub.ReceivedMessage, err error) {
	out = make([]pubsub.ReceivedMessage, len(in))
	for i, m := range in {
		var awsMsgID string
		if m.ReceiptHandle != nil {
			awsMsgID = *m.MessageId
		}
		msgID, ok := m.MessageAttributes[idAttributeKey]
		if !ok || msgID.StringValue == nil {
			return nil, fmt.Errorf("message without ID: AWS message ID %s", awsMsgID)
		}
		version, ok := m.MessageAttributes[versionAttributeKey]
		if !ok || version.StringValue == nil {
			return nil, fmt.Errorf("message without version: AWS message ID %s", awsMsgID)
		}
		var key string
		if x, ok := m.MessageAttributes[keyAttributeKey]; ok && x.StringValue != nil {
			key = *x.StringValue
		}
		var name string
		if x, ok := m.MessageAttributes[nameAttributeKey]; ok && x.StringValue != nil {
			name = *x.StringValue
		}
		var body string
		if m.Body != nil {
			body = *m.Body
		}
		var count int
		if x, ok := m.Attributes["ApproximateReceiveCount"]; ok {
			if c, err := strconv.Atoi(x); err == nil {
				count = c
			}
		}
		if x, ok := m.MessageAttributes[receiveCountAttrKey]; ok && x.StringValue != nil {
			if c, err := strconv.Atoi(*x.StringValue); err == nil {
				count += c
			}
		}
		out[i] = &message{
			id:               *msgID.StringValue,
			version:          *version.StringValue,
			key:              key,
			name:             name,
			body:             body,
			receiveCount:     count,
			attributes:       decodeCustomAttributes(m.MessageAttributes),
			sqsMessageID:     m.MessageId,
			sqsReceiptHandle: m.ReceiptHandle,
			ackNotifications: ackNotifications,
			subscriber:       s,
		}
	}
	return out, nil
}

func (s *Subscriber) ack(_ context.Context, msg *message) error {
	if !s.isRunning() {
		return fmt.Errorf("%w", ErrSubscriberStopped)
	}

	err := s.ackStrategy.Ack(context.Background(), msg)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrAcknowledgement, err)
	}

	return nil
}

func (s *Subscriber) nack(_ context.Context, msg *message) error {
	if !s.isRunning() {
		return fmt.Errorf("%w", ErrSubscriberStopped)
	}

	err := s.ackStrategy.NAck(context.Background(), msg)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrNAcknowledgement, err)
	}

	return nil
}

func (s *Subscriber) changeVisibility(ctx context.Context, msg *message, delay time.Duration) error {
	// use the storage if the delay is greater that SQS maximum.
	if delay > s.storageThreshold {
		if s.storage == nil {
			return fmt.Errorf("%w: no storage available for re-scheduling a message above %v", ErrReSchedule, s.storageThreshold)
		}

		// since we are going to publish a new message the received count will reset
		// we add the current receive count as an attribute, so we can sum it later.
		msg.Attributes()[receiveCountAttrKey] = strconv.Itoa(msg.ReceivedCount())

		err := s.storage.Schedule(ctx, time.Now().Add(delay), s.queueURL, &pubsub.Envelope{
			ID:         msg.ID(),
			Name:       msg.Name(),
			Key:        msg.Key(),
			Body:       msg.Body(),
			Version:    msg.Version(),
			Attributes: msg.Attributes(),
		})
		if err != nil {
			return fmt.Errorf("%w: %v", ErrReSchedule, err)
		}

		// ack the message so it's not redelivered.
		return s.ack(ctx, msg)
	}

	_, err := s.sqs.ChangeMessageVisibility(ctx, &sqs.ChangeMessageVisibilityInput{
		QueueUrl:          &s.queueURL,
		ReceiptHandle:     msg.sqsReceiptHandle,
		VisibilityTimeout: int32(delay.Round(time.Second).Seconds()),
	})
	if err != nil {
		return fmt.Errorf("%w: %v", ErrChangeVisibility, err)
	}

	return nil
}

func (s *Subscriber) isRunning() bool {
	s.statusMx.RLock()
	defer s.statusMx.RUnlock()

	return s.status == started
}

type sqsSvc interface {
	ReceiveMessage(ctx context.Context, params *sqs.ReceiveMessageInput, optFns ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error)
	DeleteMessage(ctx context.Context, params *sqs.DeleteMessageInput, optFns ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error)
	DeleteMessageBatch(ctx context.Context, params *sqs.DeleteMessageBatchInput, optFns ...func(*sqs.Options)) (*sqs.DeleteMessageBatchOutput, error)
	ChangeMessageVisibility(ctx context.Context, params *sqs.ChangeMessageVisibilityInput, optFns ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityOutput, error)
}
