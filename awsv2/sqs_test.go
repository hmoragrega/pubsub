//+build integration

package aws

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

func badSQS() *sqs.Client {
	cfg := aws.Config{
		Credentials: credentials.NewStaticCredentialsProvider("id", "secret", "token"),
		EndpointResolver: aws.EndpointResolverFunc(func(_, _ string) (aws.Endpoint, error) {
			return aws.Endpoint{
				URL: "http://bad-host",
			}, nil
		}),
		Retryer: func() aws.Retryer {
			return aws.NopRetryer{}
		},
		HTTPClient: &http.Client{Timeout: 25 * time.Millisecond},
		Region:     "eu-west-3",
	}
	return sqs.NewFromConfig(cfg)
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
	err := AttachQueueForwardingPolicy(context.Background(), badSQS(), "foo", "bar", "fuz")
	if err == nil {
		t.Fatal("expected error using an uninitialized sns")
	}
}
