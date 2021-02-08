package aws

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sns"
)

func NewDefaultSNS() (*sns.SNS, error) {
	sess, err := session.NewSession()
	if err != nil {
		return nil, err
	}
	return sns.New(sess), nil
}

func MustSNS(svc *sns.SNS, err error) *sns.SNS {
	if err != nil {
		panic(err)
	}
	return svc
}

func CreateTopic(ctx context.Context, svc *sns.SNS, topicName string) (string, error) {
	out, err := svc.CreateTopicWithContext(ctx, &sns.CreateTopicInput{
		Name: &topicName,
	})
	if err != nil {
		return "", fmt.Errorf("cannot create topic %s: %v", topicName, err)
	}

	return *out.TopicArn, nil
}

func DeleteTopic(ctx context.Context, svc *sns.SNS, topicARN string) error {
	_, err := svc.DeleteTopicWithContext(ctx, &sns.DeleteTopicInput{
		TopicArn: &topicARN,
	})
	if err != nil {
		return fmt.Errorf("cannot delete topic %s: %v", topicARN, err)
	}
	return err
}

func Subscribe(ctx context.Context, svc *sns.SNS, topicARN, queueARN string) (string, error) {
	out, err := svc.SubscribeWithContext(ctx, &sns.SubscribeInput{
		Endpoint: &queueARN,
		TopicArn: &topicARN,
		Attributes: map[string]*string{
			"RawMessageDelivery": aws.String("true"), // pass the raw message to SQS
		},
		Protocol: aws.String("sqs"),
	})
	if err != nil {
		return "", fmt.Errorf("cannot subscribe queue %s to topic %s: %v", queueARN, topicARN, err)
	}
	return *out.SubscriptionArn, nil
}

func Unsubscribe(ctx context.Context, svc *sns.SNS, subscriptionARN string) error {
	_, err := svc.UnsubscribeWithContext(ctx, &sns.UnsubscribeInput{
		SubscriptionArn: &subscriptionARN,
	})
	if err != nil {
		return fmt.Errorf("cannot unsubscribe subscription %s: %v", subscriptionARN, err)
	}
	return nil
}

func MustCreateResource(s string, err error) string {
	if err != nil {
		panic(err)
	}
	return s
}

func Must(err error) {
	if err != nil {
		panic(err)
	}
}
