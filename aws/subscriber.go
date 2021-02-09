package aws

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/hmoragrega/pubsub"
	"github.com/hmoragrega/workers"
)

var (
	ErrSubscriberStopped = errors.New("subscriber stopped")
	ErrAcknowledgement   = errors.New("cannot ack message")

	maxNumberOfMessages int64 = 10
	waitTimeSeconds     int64 = 20
	allAttributes             = []*string{aws.String("All")}
)

var _ pubsub.Subscriber = (*Subscriber)(nil)

type AckConfig struct {
	// Timeout for the acknowledgements request.
	// No timeout by default.
	Timeout time.Duration

	// Async will ack on the message asynchronously returning
	// immediately with success.
	//
	// Errors will be reported in the next consuming cycle.
	//
	// When the subscriber closes, it will wait until all
	// acknowledge operations finish, reporting any errors.
	Async bool

	// Batch will indicate to buffer acknowledgements
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

	// FlushEvery indicates how often the messages should be
	// acknowledged even if the batch is not full yet.
	//
	// This value has no effect if Batch is not true.
	FlushEvery time.Duration
}

// Subscriber for AWS SQS.
type Subscriber struct {
	// SQS Service.
	SQS sqsSvc

	// QueueURL for the SQS queue.
	QueueURL string

	// @TODO
	// AutoSubscribe AutoSubscribe

	// AckConfig configuration the acknowledgements behaviour
	AckConfig AckConfig

	// WorkersConfig workers pool configuration.
	WorkersConfig workers.Config

	pool        *workers.Pool
	results     chan consumeResult
	ackStrategy ackStrategy
	stopped     atomicBool
	once        sync.Once
}

func (s *Subscriber) Subscribe() error {
	if err := s.init(); err != nil {
		return err
	}
	if err := s.pool.Start(workers.JobFunc(s.consume)); err != nil {
		return err
	}

	return s.ackStrategy.Start()
}

// Next consumes the next batch of messages in the queue and
// puts them in the messages channel.
func (s *Subscriber) Next(ctx context.Context) (pubsub.ReceivedMessage, error) {
	if s.stopped.isTrue() {
		return nil, fmt.Errorf("%w", ErrSubscriberStopped)
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case res, ok := <-s.results:
		if !ok {
			return nil, ErrSubscriberStopped
		}
		return res.message, res.err
	}
}

// Stop stops consuming messages.
func (s *Subscriber) Stop(ctx context.Context) (err error) {
	s.stopped.setTrue()

	if err = s.pool.Close(ctx); err != nil {
		return err
	}

	close(s.results)

	return s.ackStrategy.Close(ctx)
}

func (s *Subscriber) init() (err error) {
	s.once.Do(func() {
		if s.SQS == nil {
			err = fmt.Errorf("SQS service not set")
			return
		}
		if s.QueueURL == "" {
			err = fmt.Errorf("QueueURL cannot be empty")
			return
		}
		s.results = make(chan consumeResult, maxNumberOfMessages)
		s.pool = workers.New()

		if s.AckConfig.Async || s.AckConfig.BatchSize > 0 {
			s.ackStrategy = newAsyncAck(s.SQS, s.QueueURL, s.AckConfig, s.WorkersConfig)
		} else {
			s.ackStrategy = newSyncAck(s.SQS, s.QueueURL)
		}
	})

	return err
}

// consumes the next batch of messages in
// the queue and puts them in the messages channel.
func (s *Subscriber) consume(ctx context.Context) error {
	out, err := s.SQS.ReceiveMessageWithContext(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:              &s.QueueURL,
		MaxNumberOfMessages:   &maxNumberOfMessages,
		WaitTimeSeconds:       &waitTimeSeconds,
		AttributeNames:        allAttributes,
		MessageAttributeNames: allAttributes,
	})
	if err != nil {
		// aws/http library does not return
		// a wrapped context.Canceled
		if ctx.Err() != nil {
			return ctx.Err()
		}
		s.results <- consumeResult{err: err}
		return err
	}
	messages, err := s.wrapMessages(out.Messages)
	if err != nil {
		s.results <- consumeResult{err: err}
		return err
	}
	for _, message := range messages {
		s.results <- consumeResult{message: message}
	}
	return nil
}

func (s *Subscriber) wrapMessages(in []*sqs.Message) (out []pubsub.ReceivedMessage, err error) {
	out = make([]pubsub.ReceivedMessage, len(in))
	for i, m := range in {
		var awsMsgID string
		if m.ReceiptHandle != nil {
			awsMsgID = *m.MessageId
		}
		msgID, ok := m.MessageAttributes[idAttributeKey]
		if !ok {
			return nil, fmt.Errorf("message without ID: AWS message ID %s", awsMsgID)
		}
		version, ok := m.MessageAttributes[versionAttributeKey]
		if !ok {
			return nil, fmt.Errorf("message without version: AWS message ID %s", awsMsgID)
		}
		var key string
		if x, ok := m.MessageAttributes[keyAttributeKey]; ok {
			key = *x.StringValue
		}
		name, ok := m.MessageAttributes[nameAttributeKey]
		if !ok {
			return nil, fmt.Errorf("message without name: AWS message ID %s", awsMsgID)
		}
		out[i] = &message{
			id:               msgID.StringValue,
			version:          version.StringValue,
			key:              &key,
			name:             name.StringValue,
			body:             []byte(*m.Body),
			attributes:       decodeAttributes(m.MessageAttributes),
			sqsMessageID:     m.MessageId,
			sqsReceiptHandle: m.ReceiptHandle,
			subscriber:       s,
		}
	}
	return out, nil
}

func (s *Subscriber) ack(_ context.Context, msg *message) error {
	if s.stopped.isTrue() {
		return fmt.Errorf("%w", ErrSubscriberStopped)
	}

	return s.ackStrategy.Ack(context.Background(), msg)
}

// ConsumeResult is the result of consuming messages from the queue.
type consumeResult struct {
	message pubsub.ReceivedMessage
	err     error
}

type atomicBool int32

func (b *atomicBool) isTrue() bool {
	return atomic.LoadInt32((*int32)(b)) != 0
}

func (b *atomicBool) setTrue() {
	atomic.StoreInt32((*int32)(b), 1)
}

func (b *atomicBool) setFalse() {
	atomic.StoreInt32((*int32)(b), 0)
}

type sqsSvc interface {
	ReceiveMessageWithContext(ctx aws.Context, input *sqs.ReceiveMessageInput, opts ...request.Option) (*sqs.ReceiveMessageOutput, error)
	DeleteMessageBatch(input *sqs.DeleteMessageBatchInput) (*sqs.DeleteMessageBatchOutput, error)
	DeleteMessageWithContext(ctx aws.Context, input *sqs.DeleteMessageInput, opts ...request.Option) (*sqs.DeleteMessageOutput, error)
}
