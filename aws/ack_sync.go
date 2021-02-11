package aws

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

type syncAck struct {
	sqs      sqsSvc
	queueURL string
}

func newSyncAck(svc sqsSvc, queueURL string) *syncAck {
	return &syncAck{
		sqs:      svc,
		queueURL: queueURL,
	}
}

func (s *syncAck) Ack(ctx context.Context, msg *message) error {
	_, err := s.sqs.DeleteMessage(ctx, &sqs.DeleteMessageInput{
		ReceiptHandle: msg.sqsReceiptHandle,
		QueueUrl:      &s.queueURL,
	})
	return err
}

func (s *syncAck) Close(_ context.Context) error {
	return nil
}
