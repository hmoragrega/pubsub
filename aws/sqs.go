package aws

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
)

func NewDefaultSQS() (*sqs.SQS, error) {
	sess, err := session.NewSession()
	if err != nil {
		return nil, err
	}
	return sqs.New(sess), nil
}

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

func DeleteQueue(ctx context.Context, svc *sqs.SQS, queueURL string) error {
	_, err := svc.DeleteQueueWithContext(ctx, &sqs.DeleteQueueInput{
		QueueUrl: aws.String(queueURL),
	})
	if err != nil {
		return fmt.Errorf("cannot delete queue %s: %v", queueURL, err)
	}
	return err
}
