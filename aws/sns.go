package aws

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sns"
)

// CreateTopic creates a SNS topic.
func CreateTopic(ctx context.Context, svc *sns.Client, topicName string) (string, error) {
	out, err := svc.CreateTopic(ctx, &sns.CreateTopicInput{
		Name: &topicName,
	})
	if err != nil {
		return "", fmt.Errorf("cannot create topic %s: %v", topicName, err)
	}

	return *out.TopicArn, nil
}

// DeleteTopic deletes a topic.
func DeleteTopic(ctx context.Context, svc *sns.Client, topicARN string) error {
	_, err := svc.DeleteTopic(ctx, &sns.DeleteTopicInput{
		TopicArn: &topicARN,
	})
	if err != nil {
		return fmt.Errorf("cannot delete topic %s: %v", topicARN, err)
	}
	return err
}

// Subscribe a queue to a topic with raw delivery enabled
func Subscribe(ctx context.Context, svc *sns.Client, topicARN, queueARN string) (string, error) {
	out, err := svc.Subscribe(ctx, &sns.SubscribeInput{
		Endpoint: &queueARN,
		TopicArn: &topicARN,
		Attributes: map[string]string{
			"RawMessageDelivery": "true", // pass the raw message to SQS
		},
		Protocol: aws.String("sqs"),
	})
	if err != nil {
		return "", fmt.Errorf("cannot subscribe queue %s to topic %s: %v", queueARN, topicARN, err)
	}
	return *out.SubscriptionArn, nil
}

// Unsubscribe removes the subscription of the topic.
func Unsubscribe(ctx context.Context, svc *sns.Client, subscriptionARN string) error {
	_, err := svc.Unsubscribe(ctx, &sns.UnsubscribeInput{
		SubscriptionArn: &subscriptionARN,
	})
	if err != nil {
		return fmt.Errorf("cannot unsubscribe subscription %s: %v", subscriptionARN, err)
	}
	return nil
}

// MustGetResource will panic if the creation of a AWS resource has failed.
func MustGetResource(s string, err error) string {
	if err != nil {
		panic(err)
	}
	return s
}

// Must will panic if wrapped operation has failed.
func Must(err error) {
	if err != nil {
		panic(err)
	}
}
