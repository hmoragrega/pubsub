package aws

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

const (
	QueueForwardingPolicyAttribute = "Policy"
	QueueRedrivePolicyAttribute    = "RedrivePolicy"
)

// CreateQueue creates a SQS queue.
// Returns the QueueURL.
func CreateQueue(ctx context.Context, svc *sqs.Client, queueName string) (string, error) {
	out, err := svc.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String(queueName),
	})
	if err != nil {
		return "", fmt.Errorf("cannot create queue %s: %v", queueName, err)
	}

	return *out.QueueUrl, nil
}

// GetQueueARN gets the queue ARN.
func GetQueueARN(ctx context.Context, svc *sqs.Client, queueURL string) (string, error) {
	out, err := svc.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		AttributeNames: []types.QueueAttributeName{"QueueArn"},
		QueueUrl:       &queueURL,
	})
	if err != nil {
		return "", fmt.Errorf("cannot get queue ARN %s: %v", queueURL, err)
	}

	return out.Attributes["QueueArn"], nil
}

// DeleteQueue deletes a queue.
func DeleteQueue(ctx context.Context, svc *sqs.Client, queueURL string) error {
	_, err := svc.DeleteQueue(ctx, &sqs.DeleteQueueInput{
		QueueUrl: aws.String(queueURL),
	})
	if err != nil {
		return fmt.Errorf("cannot delete queue %s: %v", queueURL, err)
	}
	return err
}

// SetQueueAttributes sets the queue attributes.
func SetQueueAttributes(ctx context.Context, svc *sqs.Client, queueURL string, attributes map[string]string) error {
	_, err := svc.SetQueueAttributes(ctx, &sqs.SetQueueAttributesInput{
		QueueUrl:   &queueURL,
		Attributes: attributes,
	})
	if err != nil {
		return fmt.Errorf("cannot set queue attributes: %v", err)
	}
	return nil
}

// RedrivePolicy return the string to use for the redrive policy attribute of a queue.
func RedrivePolicy(deadLetterQueueARN string, maxReceiveCount int) string {
	out, _ := json.Marshal(map[string]string{
		"deadLetterTargetArn": deadLetterQueueARN,
		"maxReceiveCount":     strconv.Itoa(maxReceiveCount),
	})
	return string(out)
}

// ForwardingPolicy generates the forwarding policy for a queue to be able to receive
// messages from the given topics.
func ForwardingPolicy(queueARN string, topicARNs ...string) string {
	statements := make([]string, len(topicARNs))
	for i, topicARN := range topicARNs {
		statements[i] = fmt.Sprintf(`{
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
`, queueARN, topicARN)
	}

	return strings.TrimSpace(fmt.Sprintf(`
{
  "Version": "2012-10-17",
  "Statement": [
	%s
  ]
}
`, strings.Join(statements, ",\n")))
}

// AttachQueueForwardingPolicy attaches a queue policy that enables a topic to send messages to it.
func AttachQueueForwardingPolicy(ctx context.Context, svc *sqs.Client, queueURL, queueARN string, topicARNs ...string) error {
	return SetQueueAttributes(ctx, svc, queueURL, map[string]string{
		QueueForwardingPolicyAttribute: ForwardingPolicy(queueARN, topicARNs...),
	})
}
