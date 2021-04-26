package aws

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

type syncAck struct {
	sqs                    sqsSvc
	queueURL               string
	changeVisibilityOnNack bool
}

func newSyncAck(svc sqsSvc, queueURL string, changeVisibilityOnNack bool) *syncAck {
	return &syncAck{
		sqs:                    svc,
		queueURL:               queueURL,
		changeVisibilityOnNack: changeVisibilityOnNack,
	}
}

func (s *syncAck) Ack(ctx context.Context, msg *message) error {
	_, err := s.sqs.DeleteMessage(ctx, &sqs.DeleteMessageInput{
		ReceiptHandle: msg.sqsReceiptHandle,
		QueueUrl:      &s.queueURL,
	})
	return err
}

func (s *syncAck) NAck(ctx context.Context, msg *message) error {
	if !s.changeVisibilityOnNack {
		return nil
	}
	_, err := s.sqs.ChangeMessageVisibility(ctx, &sqs.ChangeMessageVisibilityInput{
		ReceiptHandle:     msg.sqsReceiptHandle,
		QueueUrl:          &s.queueURL,
		VisibilityTimeout: 0,
	})
	return err
}

func (s *syncAck) Close(_ context.Context) error {
	return nil
}
