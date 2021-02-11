//+build integration

package aws

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sns"
)

func badSNS() *sns.Client {
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
	return sns.NewFromConfig(cfg)
}

func TestCreateTopicFailure(t *testing.T) {
	_, err := CreateTopic(context.Background(), badSNS(), "foo")
	if err == nil {
		t.Fatal("expected error using an uninitialized sns")
	}
}

func TestDeleteTopicFailure(t *testing.T) {
	err := DeleteTopic(context.Background(), badSNS(), "foo")
	if err == nil {
		t.Fatal("expected error using an uninitialized sns")
	}
}

func TestSubscribeFailure(t *testing.T) {
	_, err := Subscribe(context.Background(), badSNS(), "foo", "bar")
	if err == nil {
		t.Fatal("expected error using an uninitialized sns")
	}
}

func TestUnsubscribeFailure(t *testing.T) {
	err := Unsubscribe(context.Background(), badSNS(), "foo")
	if err == nil {
		t.Fatal("expected error using an uninitialized sns")
	}
}

func TestMustGetResourcePanic(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic")
		}
	}()
	MustGetResource("", errors.New("foo"))
}

func TestMustPanic(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic")
		}
	}()
	Must(errors.New("foo"))
}
