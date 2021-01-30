package pulsar

import (
	"context"
	"errors"
	"fmt"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/hmoragrega/pubsub"
	"github.com/hmoragrega/workers"
)

var (
	ErrConsumerStopped = errors.New("consumer stopped")
)

var _ pubsub.MessageConsumer = (*Consumer)(nil)

// MessageConsumer is a non thin consumer wrapper on
// pulsar's consumer.
type Consumer struct {
	pool     pool
	client   pulsar.Client
	consumer pulsar.Consumer
	options  pulsar.ConsumerOptions
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

func NewConsumer(client pulsar.Client, options pulsar.ConsumerOptions, opts ...ConsumerOption) *Consumer {
	c := &Consumer{
		client:  client,
		options: options,
		results: make(chan consumeResult),
	}
	for _, opt := range opts {
		opt(c)
	}
	if c.pool == nil {
		c.pool = workers.New()
	}
	return c
}

// Close closes the subscription.
func (c *Consumer) Start() error {
	consumer, err := c.client.Subscribe(c.options)
	if err != nil {
		return fmt.Errorf("cannot subscribe to pulsar: %w", err)
	}
	c.consumer = consumer
	return c.pool.Start(workers.JobFunc(c.consume))
}

// Next returns the next message for the topic.
func (c *Consumer) Next(ctx context.Context) (pubsub.ReceivedMessage, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case res, ok := <-c.results:
		if !ok {
			return nil, ErrConsumerStopped
		}
		return res.message, res.err
	}
}

// Close closes the subscription.
func (c *Consumer) Stop(_ context.Context) error {
	c.consumer.Close()
	return nil
}

// consumes the next batch of messages in
// the queue and puts them in the messages channel.
func (c *Consumer) consume(ctx context.Context) error {
	var res consumeResult
	_, err := c.consumer.Receive(ctx)
	if err != nil {
		res.err = err
	} else {
		res.message = nil // TODO
	}
	c.results <- res
	return err
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
