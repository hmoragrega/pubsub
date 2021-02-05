package aws

import (
	"context"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/hmoragrega/pubsub"
	"github.com/hmoragrega/workers"
)

var (
	ErrConsumerStopped = errors.New("consumer stopped")

	maxNumberOfMessages int64 = 10
	waitTimeSeconds     int64 = 20
	allAttributes             = []*string{aws.String("All")}
)

var _ pubsub.Subscriber = (*Subscriber)(nil)

// Subscriber for AWS SQS.
type Subscriber struct {
	queueURL string
	sqs      *sqs.SQS
	pool     pool
	results  chan consumeResult
}

// SubscriberOption configuration option
// for the consumer.
type SubscriberOption func(*Subscriber)

type pool interface {
	Start(job workers.Job) error
	Close(ctx context.Context) error
}

// WithPool allows to pass a custom pool.
func WithPool(pool pool) SubscriberOption {
	return func(c *Subscriber) {
		c.pool = pool
	}
}

func NewSubscriber(svc *sqs.SQS, queueURL string, opts ...SubscriberOption) *Subscriber {
	c := &Subscriber{
		sqs:      svc,
		queueURL: queueURL,
		results:  make(chan consumeResult, maxNumberOfMessages),
	}
	for _, opt := range opts {
		opt(c)
	}
	if c.pool == nil {
		c.pool = workers.New()
	}
	return c
}

func (s *Subscriber) Start() (err error) {
	return s.pool.Start(workers.JobFunc(s.consume))
}

// Next consumes the next batch of messages in the queue and
// puts them in the messages channel.
func (s *Subscriber) Next(ctx context.Context) (pubsub.ReceivedMessage, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case res, ok := <-s.results:
		if !ok {
			return nil, ErrConsumerStopped
		}
		return res.message, res.err
	}
}

// Stop stops consuming messages.
func (s *Subscriber) Stop(ctx context.Context) error {
	err := s.pool.Close(ctx)
	if err != nil {
		return err
	}

	close(s.results)
	return nil
}

// consumes the next batch of messages in
// the queue and puts them in the messages channel.
func (s *Subscriber) consume(ctx context.Context) error {
	out, err := s.sqs.ReceiveMessageWithContext(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:              &s.queueURL,
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
			id:               msgID.BinaryValue,
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

func (s *Subscriber) deleteMessage(ctx context.Context, msg *message) error {
	_, err := s.sqs.DeleteMessageWithContext(ctx, &sqs.DeleteMessageInput{
		ReceiptHandle: msg.sqsReceiptHandle,
		QueueUrl:      &s.queueURL,
	})
	if err != nil {
		return fmt.Errorf("cannot delete messages: %v", err)
	}
	return nil
}

// ConsumeResult is the result of consuming
// messages from the queue.
//
// It contains a list of consumed message
// or an error if the operation failed.
type consumeResult struct {
	message pubsub.ReceivedMessage
	err     error
}
