package aws

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/hmoragrega/pubsub"
)

var (
	maxNumberOfMessages int64 = 10
	waitTimeSeconds     int64 = 20
	allAttributes             = []*string{aws.String("All")}
)

var _ pubsub.Consumer = (*QueueConsumer)(nil)

type QueueConsumer struct {
	sqs      *sqs.SQS
	queueURL string
	results  chan pubsub.ConsumeResult
}

func NewQueueConsumer(svc *sqs.SQS, queueURL string) *QueueConsumer {
	return &QueueConsumer{
		sqs:      svc,
		queueURL: queueURL,
		results:  make(chan pubsub.ConsumeResult, maxNumberOfMessages),
	}
}

func (c *QueueConsumer) Consume() <-chan pubsub.ConsumeResult {
	return c.results
}

// Do consumes the next batch of messages in the queue and
// puts them in the messages channel.
func (c *QueueConsumer) Do(ctx context.Context) {
	out, err := c.sqs.ReceiveMessageWithContext(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:              &c.queueURL,
		MaxNumberOfMessages:   &maxNumberOfMessages,
		WaitTimeSeconds:       &waitTimeSeconds,
		AttributeNames:        allAttributes,
		MessageAttributeNames: allAttributes,
	})
	if ctx.Err() != nil {
		return
	}
	if err != nil {
		c.results <- pubsub.ConsumeResult{Err: err}
		return
	}
	if len(out.Messages) == 0 {
		return
	}
	for _, m := range out.Messages {
		msg, err := c.wrapMessage(m)
		c.results <- pubsub.ConsumeResult{Message: msg, Err: err}
	}
}

func (c *QueueConsumer) wrapMessage(in *sqs.Message) (out pubsub.Message, err error) {
	if in.MessageId == nil || len(*in.MessageId) == 0 {
		return nil, pubsub.ErrEmptyMessageID
	}
	if in.Body == nil || len(*in.Body) == 0 {
		return nil, pubsub.ErrEmptyMessageBody
	}
	return &Message{
		subscriber:       c,
		id:               in.MessageId,
		body:             []byte(*in.Body),
		attributes:       attributesFromSQS(in.MessageAttributes),
		sqsMessageID:     in.MessageId,
		sqsReceiptHandle: in.ReceiptHandle,
	}, nil
}

func (c *QueueConsumer) deleteMessage(ctx context.Context, msg *Message) error {
	_, err := c.sqs.DeleteMessageWithContext(ctx, &sqs.DeleteMessageInput{
		ReceiptHandle: msg.sqsReceiptHandle,
		QueueUrl:      &c.queueURL,
	})
	if err != nil {
		return fmt.Errorf("cannot delete messages: %v", err)
	}
	return nil
}
