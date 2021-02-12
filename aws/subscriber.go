package aws

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/hmoragrega/pubsub"
)

type status uint8

const (
	started status = iota + 1
	stopped
)

var (
	ErrSubscriberStopped = errors.New("subscriber stopped")
	ErrAcknowledgement   = errors.New("cannot ack message")
	ErrAlreadyStarted    = errors.New("already started")
	ErrAlreadyStopped    = errors.New("already stopped")
	ErrMissingConfig     = errors.New("missing configuration")

	maxNumberOfMessages int32 = 10
	waitTimeSeconds     int32 = 20
	allAttributes             = []string{"All"}
)

var _ pubsub.Subscriber = (*Subscriber)(nil)

type ackStrategy interface {
	Ack(ctx context.Context, msg *message) error
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
	sqs sqsSvc

	// QueueURL for the SQS queue.
	queueURL string

	// AckConfig configuration the acknowledgements behaviour
	ackConfig AckConfig

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

// NewSQSSubscriber creates a new SQS subscriber.
func NewSQSSubscriber(sqs sqsSvc, queueURL string, opts ...func(s *Subscriber)) *Subscriber {
	s := &Subscriber{
		sqs:      sqs,
		queueURL: queueURL,
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

	if s.ackConfig.Async || s.ackConfig.BatchSize > 0 {
		s.ackStrategy = newAsyncAck(s.sqs, s.queueURL, s.ackConfig)
	} else {
		s.ackStrategy = newSyncAck(s.sqs, s.queueURL)
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

	for {
		out, err := s.sqs.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			QueueUrl:              &s.queueURL,
			MaxNumberOfMessages:   maxNumberOfMessages,
			WaitTimeSeconds:       waitTimeSeconds,
			AttributeNames:        []types.QueueAttributeName{"All"},
			MessageAttributeNames: allAttributes,
		})
		if err != nil {
			// aws/http library does not return
			// a wrapped context.Canceled
			if ctx.Err() != nil {
				return
			}
			s.next <- pubsub.Next{Err: err}
			continue
		}

		messages, err := s.wrapMessages(out.Messages)
		if err != nil {
			s.next <- pubsub.Next{Err: err}
			continue
		}

		for _, message := range messages {
			s.next <- pubsub.Next{Message: message}
		}
	}
}

func (s *Subscriber) wrapMessages(in []types.Message) (out []pubsub.ReceivedMessage, err error) {
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
		var name string
		if x, ok := m.MessageAttributes[nameAttributeKey]; ok {
			name = *x.StringValue
		}
		if m.Body == nil {
			return nil, fmt.Errorf("message with empty body: %s", awsMsgID)
		}
		out[i] = &message{
			id:               msgID.StringValue,
			version:          version.StringValue,
			key:              &key,
			name:             &name,
			body:             []byte(*m.Body),
			attributes:       decodeCustomAttributes(m.MessageAttributes),
			sqsMessageID:     m.MessageId,
			sqsReceiptHandle: m.ReceiptHandle,
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

func (s *Subscriber) isRunning() bool {
	s.statusMx.RLock()
	defer s.statusMx.RUnlock()

	return s.status == started
}

type sqsSvc interface {
	ReceiveMessage(ctx context.Context, params *sqs.ReceiveMessageInput, optFns ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error)
	DeleteMessage(ctx context.Context, params *sqs.DeleteMessageInput, optFns ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error)
	DeleteMessageBatch(ctx context.Context, params *sqs.DeleteMessageBatchInput, optFns ...func(*sqs.Options)) (*sqs.DeleteMessageBatchOutput, error)
}
