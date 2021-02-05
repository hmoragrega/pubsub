//+build integration

package aws

import (
	"context"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sns"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/hmoragrega/workers"

	"github.com/hmoragrega/pubsub"
)

var (
	sqsTest *sqs.SQS
	snsTest *sns.SNS
)

func TestMain(m *testing.M) {
	sess, err := session.NewSessionWithOptions(session.Options{
		Config: aws.Config{
			Credentials: credentials.NewStaticCredentials("id", "secret", "token"),
			Endpoint:    aws.String(getEnvOrDefault("AWS_ENDPOINT", "localhost:4100")),
			Region:      aws.String(getEnvOrDefault("AWS_REGION", "us-east-1")),
			DisableSSL:  aws.Bool(true),
		},
	})
	if err != nil {
		panic(err)
	}
	sqsTest = sqs.New(sess)
	snsTest = sns.New(sess)
	m.Run()
}

func TestQueueConsumer_ConsumeError(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	c := NewSubscriber(sqsTest, "bad-queue")
	if err := c.Subscribe(); err != nil {
		t.Fatal("failed to start consumer", err)
	}
	t.Cleanup(func() {
		if err := c.Stop(context.Background()); err != nil {
			t.Fatal("failed to stop consumer", err)
		}
	})

	_, err := c.Next(ctx)
	if err == nil {
		t.Fatal("expected error but got nil")
	}
}

func TestQueueConsumer_JobHonorContexts(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	c := NewSubscriber(sqsTest, "foo")
	if err := c.Subscribe(); err != nil {
		t.Fatal("failed to start consumer", err)
	}
	t.Cleanup(func() {
		if err := c.Stop(context.Background()); err != nil {
			t.Fatal("failed to stop consumer", err)
		}
	})

	_, err := c.Next(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatal("unexpected error result", err)
	}
}

type testStruct struct {
	ID   int
	Name string
}

func TestPubSubIntegration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var (
		testTopic     = "pubsub-test-topic"
		eventName     = "test-struct"
		testAttribute = "test-attribute"
		topicARN      = createTestTopic(ctx, t, testTopic)
		queueURL      = createTestQueue(ctx, t, "pubsub-test-queue")
		_             = subscribeTestTopic(ctx, t, topicARN, queueURL)
		jsonMarshaler pubsub.JSONMarshaller
	)

	// Create SNS publisher
	publisher := pubsub.Publisher{
		Publisher: NewPublisher(snsTest, map[string]string{
			testTopic: topicARN,
		}),
		Marshaler: &jsonMarshaler,
	}

	jsonMarshaler.Register(eventName, &testStruct{})
	entity := &testStruct{
		ID:   123,
		Name: "John Doe",
	}

	err := publisher.Publish(ctx, testTopic, pubsub.Message{
		Name: eventName,
		Key:  strconv.Itoa(entity.ID),
		Data: entity,
		Attributes: map[string]string{
			testAttribute: "foo",
		},
	})
	if err != nil {
		t.Fatal("cannot publish message", err)
	}

	mc := NewSubscriber(
		sqsTest,
		queueURL,
		WithPool(workers.Must(workers.NewWithConfig(workers.Config{
			Min: 3,
		}))),
	)

	handler := func(_ context.Context, message *pubsub.Message) error {
		if len(message.ID) == 0 {
			return fmt.Errorf("empty message ID: %+v", message)
		}
		if message.Key != strconv.Itoa(entity.ID) {
			return fmt.Errorf("unexpected message key: %+v", message)
		}
		if message.Name != eventName {
			return fmt.Errorf("unexpected message key: %+v", message)
		}
		if message.Attributes[testAttribute] != "foo" {
			return fmt.Errorf("missing test attribute: %+v", message)
		}
		got, ok := message.Data.(*testStruct)
		if !ok {
			return fmt.Errorf("unexpected data type: %T", message.Data)
		}
		if !reflect.DeepEqual(got, entity) {
			return fmt.Errorf("received data is not equal: got %+v, want %+v", got, entity)
		}
		return nil
	}

	messageHandled := make(chan struct{})
	consumer := pubsub.Consumer{
		Subscriber: mc,
		HandlerResolver: pubsub.Dispatcher(map[string]pubsub.MessageHandler{
			eventName: pubsub.MessageHandlerFunc(handler),
		}),
		Unmarshaler: &jsonMarshaler,
		OnReceive: func(message pubsub.ReceivedMessage, err error) error {
			return err
		},
		OnUnregistered: func(message pubsub.ReceivedMessage) error {
			return fmt.Errorf("unregistered message: %+v", message)
		},
		OnUnmarshal: func(message pubsub.ReceivedMessage, err error) error {
			return err
		},
		OnHandler: func(message pubsub.ReceivedMessage, err error) error {
			return err
		},
		OnAck: func(message pubsub.ReceivedMessage, err error) error {
			if err != nil {
				return err
			}
			// all good!
			close(messageHandled)
			return nil
		},
	}

	if err := consumer.Subscribe(); err != nil {
		t.Fatal("failed to start consumer", err)
	}
	t.Cleanup(func() {
		if err := consumer.Stop(context.Background()); err != nil {
			t.Fatal("failed to stop consumer", err)
		}
	})

	consumerStopped := make(chan error)
	go func() {
		consumerStopped <- consumer.Consume(ctx)
		close(consumerStopped)
	}()

	select {
	case <-ctx.Done():
		t.Error("context timeout waiting for handling the message")
	case err := <-consumerStopped:
		if err != nil {
			t.Fatal("consumer stopped with error!", err)
		}
	case <-messageHandled:
	}

	cancel()

	select {
	case <-time.NewTimer(time.Second).C:
		t.Fatal("timeout waiting for a clean stop!")
	case err := <-consumerStopped:
		if err != nil {
			t.Fatal("consumer stopped with error!", err)
		}
	}
}

func createTestQueue(ctx context.Context, t *testing.T, queueName string) string {
	queueURL := MustCreateResource(CreateQueue(ctx, sqsTest, queueName))
	t.Cleanup(func() {
		if err := DeleteQueue(context.Background(), sqsTest, queueURL); err != nil {
			t.Fatal("cannot delete queue", err)
		}
	})
	_, err := sqsTest.PurgeQueueWithContext(ctx, &sqs.PurgeQueueInput{QueueUrl: &queueURL})
	if err != nil {
		t.Fatal("cannot purge queue", err)
	}
	return queueURL
}

func subscribeTestTopic(ctx context.Context, t *testing.T, topicARN, queueURL string) string {
	subscriptionARN := MustCreateResource(Subscribe(ctx, snsTest, topicARN, queueURL))
	t.Cleanup(func() {
		if err := Unsubscribe(context.Background(), snsTest, subscriptionARN); err != nil {
			t.Fatal("cannot unsubscribe queue", err)
		}
	})
	return subscriptionARN
}

func createTestTopic(ctx context.Context, t *testing.T, topicName string) string {
	queueURL := MustCreateResource(CreateTopic(ctx, snsTest, topicName))
	t.Cleanup(func() {
		if err := DeleteTopic(context.Background(), snsTest, queueURL); err != nil {
			t.Fatal("cannot delete topic", err)
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
