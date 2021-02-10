//+build integration

package aws

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sns"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/hmoragrega/pubsub"
	"github.com/hmoragrega/pubsub/internal/env"
	"github.com/hmoragrega/pubsub/marshaller"
)

var (
	sqsTest *sqs.SQS
	snsTest *sns.SNS
)

func TestMain(m *testing.M) {
	rand.Seed(time.Now().UnixNano())

	cfg := aws.Config{
		Credentials: credentials.NewEnvCredentials(),
		Region:      aws.String(env.GetEnvOrDefault("AWS_REGION", "eu-west-3")),
	}
	if os.Getenv("AWS") != "true" {
		cfg.Credentials = credentials.NewStaticCredentials("id", "secret", "token")
		cfg.Endpoint = aws.String(env.GetEnvOrDefault("AWS_ENDPOINT", "localhost:4100"))
		cfg.DisableSSL = aws.Bool(true)
	}
	if os.Getenv("LOGGING") == "true" {
		cfg.Logger = aws.NewDefaultLogger()
		cfg.LogLevel = aws.LogLevel(aws.LogDebug | aws.LogDebugWithHTTPBody)
	}

	sess, err := session.NewSessionWithOptions(session.Options{Config: cfg})
	if err != nil {
		panic(err)
	}
	sqsTest = sqs.New(sess)
	snsTest = sns.New(sess)
	m.Run()
}

func TestSubscribe_NextError(t *testing.T) {
	c := &Subscriber{SQS: sqsTest, QueueURL: "bad queue"}
	next, err := c.Subscribe()
	if err != nil {
		t.Fatal("failed to start consumer", err)
	}
	t.Cleanup(func() {
		if err := c.Stop(context.Background()); err != nil {
			t.Fatal("failed to stop consumer", err)
		}
	})

	n := <-next
	if n.Err == nil {
		t.Fatal("expected error but got nil")
	}
}

func TestSubscribe_SubscribeErrors(t *testing.T) {
	t.Run("invalid SQS service", func(t *testing.T) {
		s := &Subscriber{SQS: nil, QueueURL: "foo"}
		_, err := s.Subscribe()
		if !errors.Is(err, ErrMissingConfig) {
			t.Fatalf("expected config missing error; got %v", err)
		}
	})

	t.Run("invalid queue URL", func(t *testing.T) {
		s := &Subscriber{SQS: sqsTest, QueueURL: ""}
		_, err := s.Subscribe()
		if !errors.Is(err, ErrMissingConfig) {
			t.Fatalf("expected config missing error; got %v", err)
		}
	})

	t.Run("already started", func(t *testing.T) {
		s := &Subscriber{SQS: sqsTest, QueueURL: "foo"}
		_, err := s.Subscribe()
		if err != nil {
			t.Fatalf("unexpected error; got %v", err)
		}

		t.Cleanup(func() {
			_ = s.Stop(context.Background())
		})

		_, err = s.Subscribe()
		if !errors.Is(err, ErrAlreadyStarted) {
			t.Fatalf("expected already started error; got %v", err)
		}
	})
}

func TestSubscribe_StopErrors(t *testing.T) {
	t.Run("already started", func(t *testing.T) {
		s := &Subscriber{SQS: sqsTest, QueueURL: "foo"}
		_, err := s.Subscribe()
		if err != nil {
			t.Fatalf("unexpected error; got %v", err)
		}

		err = s.Stop(context.Background())
		if err != nil {
			t.Fatalf("unexpected error; got %v", err)
		}

		err = s.Stop(context.Background())
		if !errors.Is(err, ErrAlreadyStopped) {
			t.Fatalf("expected already stopped error; got %v", err)
		}
	})
	t.Run("context terminated", func(t *testing.T) {
		var svc sqsStub
		s := Subscriber{SQS: &svc, QueueURL: "foo"}

		var block chan struct{}
		svc.ReceiveMessageWithContextFunc = func(_ aws.Context, _ *sqs.ReceiveMessageInput, _ ...request.Option) (*sqs.ReceiveMessageOutput, error) {
			<-block
			return nil, nil
		}

		_, err := s.Subscribe()
		if err != nil {
			t.Fatalf("unexpected error; got %v", err)
		}

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		err = s.Stop(ctx)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected conext cancelled error; got %v", err)
		}
	})
}

func TestSubscribe_AckFailsOnStoppedSubscribers(t *testing.T) {
	var s Subscriber
	err := s.ack(&message{})
	if !errors.Is(err, ErrSubscriberStopped) {
		t.Fatalf("expected already stopped error; got %v", err)
	}
}

func TestSubscribe_NextErrors(t *testing.T) {
	t.Run("poisonous messages", func(t *testing.T) {
		var svc sqsStub
		s := Subscriber{SQS: &svc, QueueURL: "foo"}

		poisonousMessages := []*sqs.Message{
			{}, // missing id
			{MessageAttributes: map[string]*sqs.MessageAttributeValue{
				idAttributeKey: {},
			}}, // missing version
			{MessageAttributes: map[string]*sqs.MessageAttributeValue{
				idAttributeKey:      {},
				versionAttributeKey: {},
			}}, // missing name
		}

		svc.ReceiveMessageWithContextFunc = func(ctx aws.Context, _ *sqs.ReceiveMessageInput, _ ...request.Option) (*sqs.ReceiveMessageOutput, error) {
			if len(poisonousMessages) == 0 {
				<-ctx.Done()
				return nil, ctx.Err()
			}

			// pop the message
			m := poisonousMessages[0]
			poisonousMessages = poisonousMessages[1:]

			return &sqs.ReceiveMessageOutput{Messages: []*sqs.Message{m}}, nil
		}

		next, err := s.Subscribe()
		if err != nil {
			t.Fatalf("unexpected error; got %v", err)
		}

		for i := 0; i < 3; i++ {
			n := <-next
			if n.Err == nil {
				t.Fatalf("expected error from bad message; got %v", err)
			}
		}

		err = s.Stop(context.Background())
		if err != nil {
			t.Fatalf("unexpected error stopping; got %v", err)
		}
	})
}

type testStruct struct {
	ID   int
	Name string
}

func TestPubSubIntegration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	var (
		testTopic      = fmt.Sprintf("aws-pubsub-integration-%d", rand.Int31())
		testQueue      = fmt.Sprintf("%s-queue", testTopic)
		eventName      = "test-struct"
		testAttribute  = "test-attribute"
		topicARN       = createTestTopic(ctx, t, testTopic)
		queueURL       = createTestQueue(ctx, t, testQueue)
		queueARN       = MustGetResource(GetQueueARN(ctx, sqsTest, queueURL))
		jsonMarshaller marshaller.JSONMarshaller
	)

	subscribeTestTopic(ctx, t, topicARN, queueARN)
	Must(CreateForwardingPolicy(ctx, sqsTest, queueURL, queueARN, topicARN))

	// Create SNS publisher
	publisher := pubsub.Publisher{
		Publisher: &Publisher{SNS: snsTest, TopicARNs: map[string]string{
			testTopic: topicARN,
		}},
		Marshaller: &jsonMarshaller,
	}

	jsonMarshaller.Register(eventName, &testStruct{})
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

	mc := &Subscriber{
		SQS:      sqsTest,
		QueueURL: queueURL,
	}

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
	router := pubsub.Router{
		Unmarshaller: &jsonMarshaller,
		StopTimeout:  time.Second,
		OnReceive: func(_ context.Context, _ string, _ pubsub.ReceivedMessage, err error) error {
			return err
		},
		OnUnmarshal: func(_ context.Context, _ string, _ pubsub.ReceivedMessage, err error) error {
			return err
		},
		OnHandler: func(_ context.Context, _ string, _ pubsub.ReceivedMessage, err error) error {
			return err
		},
		OnAck: func(_ context.Context, _ string, _ pubsub.ReceivedMessage, err error) error {
			if err != nil {
				return err
			}
			// all good!
			close(messageHandled)
			return nil
		},
	}
	err = router.Register("topic", mc, pubsub.HandlerFunc(handler))
	if err != nil {
		t.Fatal("cannot register handler", err)
	}

	routerResult := make(chan error)
	go func() {
		routerResult <- router.Run(ctx)
	}()

	select {
	case <-ctx.Done():
		t.Error("context timeout waiting for handling the message")
	case <-messageHandled:
		cancel()
		err = <-routerResult
		if err != nil {
			t.Fatal("router stopped with error!", err)
		}
	}
}

func TestSubscriberAsyncAck(t *testing.T) {
	var (
		batchSize = 3
		messages  = 4
	)

	type receiveResult struct {
		out *sqs.ReceiveMessageOutput
		err error
	}
	type deleteBatchResult struct {
		out *sqs.DeleteMessageBatchOutput
		err error
	}
	var (
		receiveReturns     = make(chan receiveResult, 1)
		deleteBatchReturns = make(chan deleteBatchResult, 2)
		ackOperations      = make(chan struct{}, 2)
	)
	sqsSvc := &sqsStub{
		ReceiveMessageWithContextFunc: func(ctx aws.Context, _ *sqs.ReceiveMessageInput, _ ...request.Option) (*sqs.ReceiveMessageOutput, error) {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case r := <-receiveReturns:
				return r.out, r.err
			}
		},
		DeleteMessageBatchFunc: func(input *sqs.DeleteMessageBatchInput) (*sqs.DeleteMessageBatchOutput, error) {
			r := <-deleteBatchReturns
			ackOperations <- struct{}{}
			return r.out, r.err
		},
	}

	mc := &Subscriber{
		SQS:      sqsSvc,
		QueueURL: "foo",
		AckConfig: AckConfig{
			BatchSize: batchSize,
		},
	}

	input := make([]*sqs.Message, messages)
	for i := 0; i < messages; i++ {
		input[i] = &sqs.Message{
			Body: aws.String("body"),
			MessageAttributes: map[string]*sqs.MessageAttributeValue{
				idAttributeKey:      {StringValue: aws.String(fmt.Sprintf("id-%d", i))},
				versionAttributeKey: {StringValue: aws.String(fmt.Sprintf("version"))},
				nameAttributeKey:    {StringValue: aws.String(fmt.Sprintf("name"))},
			},
			ReceiptHandle: aws.String(fmt.Sprintf("handle-%d", i)),
			MessageId:     aws.String("message-id"),
		}
	}
	// Receive 4 messages
	receiveReturns <- receiveResult{out: &sqs.ReceiveMessageOutput{Messages: input}}

	// Fail to ack two messages, the errors should
	// be reported on first ack after the batch flush.
	deleteBatchReturns <- deleteBatchResult{
		out: &sqs.DeleteMessageBatchOutput{
			Failed: []*sqs.BatchResultErrorEntry{{
				Id:      aws.String("id-0"),
				Code:    aws.String("ERR-0"),
				Message: aws.String("error zero"),
			}, {
				Id:      aws.String("id-2"),
				Code:    aws.String("ERR-2"),
				Message: aws.String("error two"),
			}},
		},
	}

	// Fail with an error for the last message, this must
	// be reported on closing the subscription.
	deleteBatchReturns <- deleteBatchResult{err: errors.New("request failed")}

	next, err := mc.Subscribe()
	if err != nil {
		t.Fatal("cannot subscribe", err)
	}

	ctx := context.Background()
	for i := 0; i < batchSize+1; i++ {
		n := <-next
		if n.Err != nil {
			t.Fatalf("no error is expected consuming the messages, got :%v", err)
		}
		if i == batchSize {
			// give time for the first ack operation
			// before requesting the last message ack
			<-ackOperations
			// even after ack, we cannot be sure the error
			// will be propagated upstream in time.
			time.Sleep(50 * time.Millisecond)
		}
		err = n.Message.Ack(ctx)
		if i < batchSize {
			if err != nil {
				t.Fatalf("no error should be reported until the first batch is acked, got :%v", err)
			}
			continue
		}
		if !errors.Is(err, ErrAcknowledgement) {
			t.Fatal("expected ack error on the first messages after the first batch")
		}
	}

	err = mc.Stop(ctx)
	if !errors.Is(err, ErrAcknowledgement) {
		t.Fatal("expected ack error stopping the subscription")
	}
}

func TestSubscriberAsyncAckTicker(t *testing.T) {
	waitForFlush := make(chan struct{})

	var nextCalls int
	sqsSvc := &sqsStub{
		ReceiveMessageWithContextFunc: func(ctx aws.Context, _ *sqs.ReceiveMessageInput, _ ...request.Option) (*sqs.ReceiveMessageOutput, error) {
			nextCalls++
			if nextCalls == 1 {
				return &sqs.ReceiveMessageOutput{
					Messages: []*sqs.Message{{
						Body: aws.String("body"),
						MessageAttributes: map[string]*sqs.MessageAttributeValue{
							idAttributeKey:      {StringValue: aws.String("id")},
							versionAttributeKey: {StringValue: aws.String("version")},
							nameAttributeKey:    {StringValue: aws.String("name")},
						},
						ReceiptHandle: aws.String("handle"),
						MessageId:     aws.String("message-id"),
					}},
				}, nil
			}
			<-ctx.Done()
			return nil, ctx.Err()
		},
		DeleteMessageBatchFunc: func(input *sqs.DeleteMessageBatchInput) (*sqs.DeleteMessageBatchOutput, error) {
			close(waitForFlush)
			return nil, errors.New("request failed")
		},
	}

	s := &Subscriber{
		SQS:      sqsSvc,
		QueueURL: "foo",
		AckConfig: AckConfig{
			Async:      true,
			BatchSize:  2,
			FlushEvery: 25 * time.Millisecond,
		},
	}

	next, err := s.Subscribe()
	if err != nil {
		t.Fatal("cannot subscribe", err)
	}

	ctx := context.Background()
	n := <-next
	m, err := n.Message, n.Err
	if err != nil {
		t.Fatalf("no error is expected consuming the messages, got :%v", err)
	}
	err = m.Ack(ctx)
	if err != nil {
		t.Fatalf("no error should be reported :%v", err)
	}

	<-waitForFlush

	err = s.Stop(ctx)
	if !errors.Is(err, ErrAcknowledgement) {
		t.Fatalf("expected ack error reported on stopping, got: %v", ErrAcknowledgement)
	}
}

func createTestQueue(ctx context.Context, t *testing.T, queueName string) string {
	queueURL := MustGetResource(CreateQueue(ctx, sqsTest, queueName))
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
	subscriptionARN := MustGetResource(Subscribe(ctx, snsTest, topicARN, queueURL))
	t.Cleanup(func() {
		if err := Unsubscribe(context.Background(), snsTest, subscriptionARN); err != nil {
			t.Fatal(err)
		}
	})
	return subscriptionARN
}

func createTestTopic(ctx context.Context, t *testing.T, topicName string) string {
	queueURL := MustGetResource(CreateTopic(ctx, snsTest, topicName))
	t.Cleanup(func() {
		if err := DeleteTopic(context.Background(), snsTest, queueURL); err != nil {
			t.Fatal(err)
		}
	})
	return queueURL
}

type sqsStub struct {
	ReceiveMessageWithContextFunc func(ctx aws.Context, input *sqs.ReceiveMessageInput, opts ...request.Option) (*sqs.ReceiveMessageOutput, error)
	DeleteMessageBatchFunc        func(input *sqs.DeleteMessageBatchInput) (*sqs.DeleteMessageBatchOutput, error)
	DeleteMessageWithContextFunc  func(ctx aws.Context, input *sqs.DeleteMessageInput, opts ...request.Option) (*sqs.DeleteMessageOutput, error)
}

func (s *sqsStub) ReceiveMessageWithContext(ctx aws.Context, input *sqs.ReceiveMessageInput, opts ...request.Option) (*sqs.ReceiveMessageOutput, error) {
	return s.ReceiveMessageWithContextFunc(ctx, input, opts...)
}

func (s *sqsStub) DeleteMessageBatch(input *sqs.DeleteMessageBatchInput) (*sqs.DeleteMessageBatchOutput, error) {
	return s.DeleteMessageBatchFunc(input)
}

func (s *sqsStub) DeleteMessageWithContext(ctx aws.Context, input *sqs.DeleteMessageInput, opts ...request.Option) (*sqs.DeleteMessageOutput, error) {
	return s.DeleteMessageWithContext(ctx, input, opts...)
}
