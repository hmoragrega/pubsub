package aws

import (
	"context"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sns"
	"github.com/aws/aws-sdk-go/service/sqs"
)

var (
	ErrCreatingQueue    = errors.New("cannot create queue")
	ErrDeletingQueue    = errors.New("cannot delete queue")
	ErrSubscribingQueue = errors.New("cannot subscribe queue")
)

func CreateQueue(ctx context.Context, svc *sqs.SQS, queueName string) (string, error) {
	out, err := svc.CreateQueueWithContext(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String(queueName),
	})
	if err != nil {
		return "", fmt.Errorf("%w: %v", ErrCreatingQueue, err)
	}

	return *out.QueueUrl, nil
}

func DeleteQueue(ctx context.Context, svc *sqs.SQS, queueURL string) error {
	_, err := svc.DeleteQueueWithContext(ctx, &sqs.DeleteQueueInput{
		QueueUrl: aws.String(queueURL),
	})
	if err != nil {
		return fmt.Errorf("%w: %v", ErrDeletingQueue, err)
	}
	return err
}

func SubscribeQueue(ctx context.Context, svc *sns.SNS, topicARN, queueURL string, attributes map[string]string) (string, error) {
	out, err := svc.SubscribeWithContext(ctx, &sns.SubscribeInput{
		Endpoint:   &queueURL,
		Attributes: attributesToPtr(attributes),
		Protocol:   aws.String("sqs"),
		TopicArn:   &topicARN,
	})
	if err != nil {
		return "", fmt.Errorf("%w: %v", ErrSubscribingQueue, err)
	}
	return *out.SubscriptionArn, nil
}

func MustCreateQueue(s string, err error) string {
	if err != nil {
		panic(err)
	}
	return s
}

func attributesToPtr(in map[string]string) map[string]*string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]*string, len(in))
	for k, v := range in {
		out[k] = &v
	}
	return out
}
