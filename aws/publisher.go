package aws

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sqs"

	"github.com/hmoragrega/pubsub"
)

var ErrTopicOrQueueNotFound = errors.New("could not find neither topic ARN nor queue URL")

// Publisher SNS+SQS publisher.
type Publisher struct {
	sns       *sns.Client
	sqs       *sqs.Client
	resources sync.Map
}

// NewPublisher creates a new SNS+SQS publisher.
func NewPublisher(sns *sns.Client, sqs *sqs.Client, resources map[string]string) *Publisher {
	p := &Publisher{
		sns: sns,
		sqs: sqs,
	}
	for k, v := range resources {
		p.resources.Store(k, v)
	}
	return p
}

// Publish a message trough SNS.
func (p *Publisher) Publish(ctx context.Context, resourceID string, envelopes ...*pubsub.Envelope) error {
	// If the resource exists we get it, otherwise we use the identifier.
	resource := resourceID
	if r, ok := p.resources.Load(resourceID); ok {
		resource = r.(string)
	}

	// Note: topic ARN "are" technically URLs, so this check need to go first.
	if topicARN := resource; isSNSTopicARN(topicARN) {
		return publishSNSMessage(ctx, p.sns, topicARN, envelopes...)
	}

	if queueURL := resource; isURL(queueURL) {
		return publishSQSMessage(ctx, p.sqs, queueURL, 0, envelopes...)
	}

	return fmt.Errorf("%w: %s", ErrTopicOrQueueNotFound, resource)
}

func (p *Publisher) AddResource(resourceID, resource string) {
	p.resources.Store(resourceID, resource)
}

func publishSNSMessage(ctx context.Context, c *sns.Client, topicARN string, envelopes ...*pubsub.Envelope) error {
	for _, env := range envelopes {
		// every FIFO queue message needs to have a message group in SNS
		// @TODO only for FIFO
		// key := env.Key
		// if key == "" {
		//		key = "void"
		//	}

		_, err := c.Publish(ctx, &sns.PublishInput{
			TopicArn:          &topicARN,
			Message:           stringPtr(env.Body),
			MessageAttributes: encodeSNSAttributes(env),
			//MessageDeduplicationId: &base64ID, // @TODO FIFO only
			//MessageGroupId:         &key,      // @TODO FIFO only
		})
		if err != nil {
			return fmt.Errorf("cannot publish message %s: %w", env.ID, wrapError(err))
		}
	}
	return nil
}
