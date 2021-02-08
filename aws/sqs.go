package aws

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
)

func MustSQS(svc *sqs.SQS, err error) *sqs.SQS {
	if err != nil {
		panic(err)
	}
	return svc
}

func CreateQueue(ctx context.Context, svc *sqs.SQS, queueName string) (string, error) {
	out, err := svc.CreateQueueWithContext(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String(queueName),
	})
	if err != nil {
		return "", fmt.Errorf("cannot create queue %s: %v", queueName, err)
	}

	return *out.QueueUrl, nil
}

func GetQueueARN(ctx context.Context, svc *sqs.SQS, queueURL string) (string, error) {
	out, err := svc.GetQueueAttributesWithContext(ctx, &sqs.GetQueueAttributesInput{
		AttributeNames: []*string{aws.String("QueueArn")},
		QueueUrl:       &queueURL,
	})
	if err != nil {
		return "", fmt.Errorf("cannot get queue ARN %s: %v", queueURL, err)
	}

	return *out.Attributes["QueueArn"], nil
}

// PurgeQueue note: there is a cool down period of 60s before
// the queue can be purged again.
func PurgeQueue(ctx context.Context, svc *sqs.SQS, queueURL string) error {
	_, err := svc.PurgeQueueWithContext(ctx, &sqs.PurgeQueueInput{
		QueueUrl: &queueURL,
	})
	if err != nil {
		return fmt.Errorf("cannot purge ARN %s: %v", queueURL, err)
	}

	return nil
}

func DeleteQueue(ctx context.Context, svc *sqs.SQS, queueURL string) error {
	_, err := svc.DeleteQueueWithContext(ctx, &sqs.DeleteQueueInput{
		QueueUrl: aws.String(queueURL),
	})
	if err != nil {
		return fmt.Errorf("cannot delete queue %s: %v", queueURL, err)
	}
	return err
}
