//+build integration

package aws

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
)

func badSQS() *sqs.SQS {
	cfg := aws.Config{
		Credentials: credentials.NewStaticCredentials("id", "secret", "token"),
		Endpoint:    aws.String("bad-host"),
		Region:      aws.String("eu-west-3"),
		Logger:      nil,
	}
	sess, err := session.NewSessionWithOptions(session.Options{Config: cfg})
	if err != nil {
		panic(err)
	}
	return sqs.New(sess)
}

func TestCreateQueueFailure(t *testing.T) {
	_, err := CreateQueue(context.Background(), badSQS(), "foo")
	if err == nil {
		t.Fatal("expected error using an uninitialized sns")
	}
}

func TestGetQueueARNFailure(t *testing.T) {
	_, err := GetQueueARN(context.Background(), badSQS(), "foo")
	if err == nil {
		t.Fatal("expected error using an uninitialized sns")
	}
}

func TestDeleteQueueFailure(t *testing.T) {
	err := DeleteQueue(context.Background(), badSQS(), "foo")
	if err == nil {
		t.Fatal("expected error using an uninitialized sns")
	}
}

func TestCreateForwardingPolicyFailure(t *testing.T) {
	err := CreateForwardingPolicy(context.Background(), badSQS(), "foo", "bar", "fuz")
	if err == nil {
		t.Fatal("expected error using an uninitialized sns")
	}
}
