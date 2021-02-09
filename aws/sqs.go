package aws

import (
	"context"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
)

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

func CreateForwardingPolicy(ctx context.Context, svc *sqs.SQS, queueURL, queueARN, topicARN string) error {
	_, err := svc.SetQueueAttributesWithContext(ctx, &sqs.SetQueueAttributesInput{
		QueueUrl: &queueURL,
		Attributes: map[string]*string{
			"Policy": aws.String(strings.TrimSpace(fmt.Sprintf(`
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": "*",
      "Action": "sqs:SendMessage",
      "Resource": "%s",
      "Condition": {
        "ArnEquals": {
          "aws:SourceArn": "%s"
        }
      }
    }
  ]
}
`, queueARN, topicARN)))}})

	if err != nil {
		return fmt.Errorf("cannot create forwarding policy: %v", err)
	}
	return nil
}
