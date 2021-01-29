//+build integration

package aws

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/hmoragrega/pubsub"
)

var sqsTestInstance *sqs.SQS

const testRequestIDKey = "Request-ID"

func TestMain(m *testing.M) {
	sess, err := session.NewSessionWithOptions(session.Options{
		Config: aws.Config{
			Credentials: credentials.NewStaticCredentials("id", "secret", "token"),
			Endpoint:    aws.String(getEnvOrDefault("LOCALSTACK_ENDPOINT", "localhost:4566")),
			Region:      aws.String(getEnvOrDefault("LOCALSTACK_REGION", "us-east-1")),
			DisableSSL:  aws.Bool(true),
		},
	})
	if err != nil {
		panic(err)
	}
	sqsTestInstance = sqs.New(sess)
	m.Run()
}

func TestQueueConsumer_Consume(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	queueURL := createTestQueue(ctx, t, "test-subscriber-subscribe")
	s := NewQueueConsumer(sqsTestInstance, queueURL)

	var (
		wantReqID = "123"
		wantBody  = "body"
	)

	_, err := sqsTestInstance.SendMessageWithContext(ctx, &sqs.SendMessageInput{
		MessageAttributes: map[string]*sqs.MessageAttributeValue{
			testRequestIDKey: {
				DataType:    aws.String("String"),
				StringValue: &wantReqID,
			},
		},
		MessageBody: &wantBody,
		QueueUrl:    &queueURL,
	})
	if err != nil {
		t.Fatal("cannot send message", err)
	}

	// consume once.
	s.Do(ctx)

	select {
	case <-ctx.Done():
		t.Fatal("timeout waiting for the message", ctx.Err())
	case res := <-s.Consume():
		if res.Err != nil {
			t.Fatal("error receiving message", res.Err)
		}
		m := res.Message
		if got := string(m.Body()); got != wantBody {
			t.Fatalf("message body does not match, got %s, want %s", got, wantBody)
		}
		if got := m.ID(); got == "" {
			t.Fatalf("message ID is empty")
		}
		if got, _ := m.Attribute(testRequestIDKey); got != wantReqID {
			t.Fatalf("request ID attribute is empty")
		}
		if err := m.Ack(ctx); err != nil {
			t.Fatal("cannot ack message", err)
		}
	}
}

func TestQueueConsumer_ConsumeError(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	s := NewQueueConsumer(sqsTestInstance, "bad-queue")

	// consume once
	s.Do(ctx)

	select {
	case <-ctx.Done():
		t.Fatal("timeout waiting for the message", ctx.Err())
	case res := <-s.Consume():
		if res.Err == nil {
			t.Fatal("expected error but got nil", res.Err)
		}
	}
}

func TestQueueConsumer_JobHonorContexts(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	s := NewQueueConsumer(sqsTestInstance, "foo")

	// consume once with context cancelled.
	s.Do(ctx)

	var res pubsub.ConsumeResult

	select {
	case res = <-s.Consume():
		t.Fatal("unexpected consume result", res)
	default:
	}
}

func createTestQueue(ctx context.Context, t *testing.T, queueName string) string {
	queueURL := MustCreateQueue(CreateQueue(ctx, sqsTestInstance, queueName))
	t.Cleanup(func() {
		if err := DeleteQueue(context.Background(), sqsTestInstance, queueURL); err != nil {
			t.Fatal("cannot delete queue", err)
		}
	})
	return queueURL
}

func getEnvOrDefault(key string, d string) string {
	s := os.Getenv(key)
	if s == "" {
		return d
	}
	return s
}
