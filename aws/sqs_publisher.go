package aws

import (
	"context"
	"errors"
	"fmt"
	"net/url"

	"github.com/aws/aws-sdk-go-v2/service/sqs"

	"github.com/hmoragrega/pubsub"
)

var ErrQueueNotFound = errors.New("could not find queue URL")

// SQSPublisher a publisher that publishes directly to queues.
type SQSPublisher struct {
	queueURLs map[string]string
	sqs       *sqs.Client
}

// NewSQSDirectPublisher creates a new SQS publisher without any queue alias.
func NewSQSDirectPublisher(sqs *sqs.Client) *SQSPublisher {
	return NewSQSPublisher(sqs, make(map[string]string, 0))
}

// NewSQSPublisher creates a new SQS publisher with a custom map for queue URLs.
func NewSQSPublisher(sqs *sqs.Client, queueURLs map[string]string) *SQSPublisher {
	return &SQSPublisher{
		sqs:       sqs,
		queueURLs: queueURLs,
	}
}

// Publish a message to a SQS queue.
func (p *SQSPublisher) Publish(ctx context.Context, queue string, envelopes ...*pubsub.Envelope) error {
	return p.publishWithDelay(ctx, queue, 0, envelopes...)
}

func (p *SQSPublisher) publishWithDelay(ctx context.Context, queue string, delaySeconds int32, envelopes ...*pubsub.Envelope) error {
	queueURL, err := p.queueURL(queue)
	if err != nil {
		return err
	}

	return publishSQSMessage(ctx, p.sqs, queueURL, delaySeconds, envelopes...)
}

func publishSQSMessage(ctx context.Context, c *sqs.Client, queueURL string, delaySeconds int32, envelopes ...*pubsub.Envelope) error {
	for _, env := range envelopes {
		_, err := c.SendMessage(ctx, &sqs.SendMessageInput{
			QueueUrl:          &queueURL,
			MessageBody:       stringPtr(env.Body),
			MessageAttributes: encodeSQSAttributes(env),
			DelaySeconds:      delaySeconds,
		})
		if err != nil {
			return fmt.Errorf("cannot publish message %s: %w", env.ID, err)
		}
	}
	return nil
}

func (p *SQSPublisher) queueURL(queue string) (string, error) {
	if isURL(queue) {
		return queue, nil
	}

	queueURL, ok := p.queueURLs[queue]
	if !ok {
		return "", fmt.Errorf("%w: %s", ErrQueueNotFound, queue)
	}

	return queueURL, nil
}

func isURL(queue string) bool {
	u, err := url.Parse(queue)

	return err == nil && u.IsAbs()
}
