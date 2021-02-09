//+build integration

package aws

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sns"
)

func badSNS() *sns.SNS {
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
	return sns.New(sess)
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
