package pulsar

import (
	"context"
	"encoding/base32"
	"encoding/base64"
	"errors"
	"fmt"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/hmoragrega/pubsub"
	"github.com/hmoragrega/workers"
)

var (
	errConsumerStopped = errors.New("consumer stopped")
)

var _ pubsub.MessageConsumer = (*Consumer)(nil)

// MessageConsumer for AWS SQS.
type Consumer struct {
	queueURL string
	consumer pulsar.Consumer
	pool     pool
	results  chan consumeResult
}

// ConsumerOption configuration option
// for the consumer.
type ConsumerOption func(*Consumer)

type pool interface {
	Start(job workers.Job) error
	Close(ctx context.Context) error
}

// WithPool allows to pass a custom pool.
func WithPool(pool pool) ConsumerOption {
	return func(c *Consumer) {
		c.pool = pool
	}
}

func NewConsumer(consumer pulsar.Consumer, queueURL string, opts ...ConsumerOption) *Consumer {
	c := &Consumer{
		consumer: consumer,
		queueURL: queueURL,
		results:  make(chan consumeResult),
	}
	for _, opt := range opts {
		opt(c)
	}
	if c.pool == nil {
		c.pool = workers.New()
	}
	return c
}

func (c *Consumer) Start() (err error) {
	return c.pool.Start(workers.JobFunc(c.consume))
}

// Next consumes the next batch of messages in the queue and
// puts them in the messages channel.
func (c *Consumer) Next(ctx context.Context) (pubsub.ReceivedMessage, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case res, ok := <-c.results:
		if !ok {
			return nil, errConsumerStopped
		}
		return res.message, res.err
	}
}

// Stop stops consuming messages.
func (c *Consumer) Stop(ctx context.Context) error {
	err := c.pool.Close(ctx)
	if err != nil {
		return err
	}

	close(c.results)
	return nil
}

// consumes the next batch of messages in
// the queue and puts them in the messages channel.
func (c *Consumer) consume(ctx context.Context) error {
	msg, err := c.consumer.Receive(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		c.results <- consumeResult{err: err}
		return err
	}

	message, err := c.wrapMessage(msg)
	if err != nil {
		c.results <- consumeResult{err: err}
		return err
	}

	c.results <- consumeResult{message: message}
	return nil
}

func (c *Consumer) wrapMessage(m pulsar.Message) (out pubsub.ReceivedMessage, err error) {
	var pulsarMsgID string
	if x, ok := m.ID().(fmt.Stringer); ok {
		pulsarMsgID = x.String()
	} else {
		pulsarMsgID = base32.HexEncoding.EncodeToString(m.ID().Serialize())
	}

	encodedID, ok := m.Properties()[idAttributeKey]
	if !ok {
		return nil, fmt.Errorf("message without ID: Pulsar message ID %s", pulsarMsgID)
	}
	version, ok := m.Properties()[versionAttributeKey]
	if !ok {
		return nil, fmt.Errorf("message without version: Pulsar message ID %s", pulsarMsgID)
	}
	var key string
	if x, ok := m.Properties()[keyAttributeKey]; ok {
		key = x
	}
	name, ok := m.Properties()[nameAttributeKey]
	if !ok {
		return nil, fmt.Errorf("message without name: Pulsar message ID %s", pulsarMsgID)
	}
	id, err := base64.StdEncoding.DecodeString(encodedID)
	if err != nil {
		return nil, fmt.Errorf("cannot decode message ID %q: %v", encodedID, err)
	}

	return &message{
		id:            id,
		version:       version,
		key:           key,
		name:          name,
		body:          m.Payload(),
		attributes:    decodeAttributes(m.Properties()),
		consumer:      c.consumer,
		pulsarMessage: m,
	}, nil
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
