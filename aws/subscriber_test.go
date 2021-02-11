//+build integration

package aws

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"os"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/aws/smithy-go/logging"
	"github.com/hmoragrega/pubsub"
	"github.com/hmoragrega/pubsub/marshaller"
)

var (
	sqsTest *sqs.Client
	snsTest *sns.Client
)

func TestMain(m *testing.M) {
	rand.Seed(time.Now().UnixNano())

	opts := []func(*config.LoadOptions) error{
		config.WithRegion("eu-west-3"),
	}
	if os.Getenv("AWS") != "true" {
		opts = append(opts,
			config.WithCredentialsProvider(
				credentials.NewStaticCredentialsProvider("id", "secret", "token"),
			),
			config.WithEndpointResolver(aws.EndpointResolverFunc(func(service, region string) (aws.Endpoint, error) {
				return aws.Endpoint{
					URL:               "http://" + getEnvOrDefault("AWS_ENDPOINT", "localhost:4100"),
					PartitionID:       "aws",
					HostnameImmutable: true,
				}, nil
			}),
			))
	}
	if os.Getenv("DEBUG") == "true" {
		opts = append(opts, config.WithLogger(logging.NewStandardLogger(os.Stderr)))
	}
	cfg, err := config.LoadDefaultConfig(context.Background(), opts...)
	if err != nil {
		log.Fatalf("unable to load SDK config, %v", err)
	}

	sqsTest = sqs.NewFromConfig(cfg)
	snsTest = sns.NewFromConfig(cfg)
	m.Run()
}

func TestSubscribe_SubscribeErrors(t *testing.T) {
	t.Run("invalid SQS service", func(t *testing.T) {
		s := &Subscriber{sqs: nil, queueURL: "foo"}
		_, err := s.Subscribe()
		if !errors.Is(err, ErrMissingConfig) {
			t.Fatalf("expected config missing error; got %v", err)
		}
	})

	t.Run("invalid queue URL", func(t *testing.T) {
		s := &Subscriber{sqs: sqsTest, queueURL: ""}
		_, err := s.Subscribe()
		if !errors.Is(err, ErrMissingConfig) {
			t.Fatalf("expected config missing error; got %v", err)
		}
	})

	t.Run("already started", func(t *testing.T) {
		s := &Subscriber{sqs: sqsTest, queueURL: "foo"}
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
		s := &Subscriber{sqs: sqsTest, queueURL: "foo"}
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
		s := Subscriber{sqs: &svc, queueURL: "foo"}

		var block chan struct{}
		svc.ReceiveMessageFunc = func(_ context.Context, _ *sqs.ReceiveMessageInput, _ ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
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
	err := s.ack(context.Background(), &message{})
	if !errors.Is(err, ErrSubscriberStopped) {
		t.Fatalf("expected already stopped error; got %v", err)
	}
}

func TestSubscribe_NextErrors(t *testing.T) {
	t.Run("non existent queue", func(t *testing.T) {
		c := &Subscriber{sqs: sqsTest, queueURL: "bad queue"}
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
	})

	t.Run("poisonous messages", func(t *testing.T) {
		var svc sqsStub
		s := Subscriber{sqs: &svc, queueURL: "foo"}

		poisonousMessages := []types.Message{
			{}, // missing id
			{MessageAttributes: map[string]types.MessageAttributeValue{
				idAttributeKey: {},
			}}, // missing version
			{MessageAttributes: map[string]types.MessageAttributeValue{
				idAttributeKey:      {},
				versionAttributeKey: {},
			}}, // missing name
		}

		svc.ReceiveMessageFunc = func(ctx context.Context, _ *sqs.ReceiveMessageInput, _ ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
			if len(poisonousMessages) == 0 {
				<-ctx.Done()
				return nil, ctx.Err()
			}

			// pop the message
			m := poisonousMessages[0]
			poisonousMessages = poisonousMessages[1:]

			return &sqs.ReceiveMessageOutput{Messages: []types.Message{m}}, nil
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
	Must(AttachQueueForwardingPolicy(ctx, sqsTest, queueURL, queueARN, topicARN))

	// Create SNS publisher
	publisher := pubsub.NewPublisher(
		NewSNSPublisher(snsTest, map[string]string{
			testTopic: topicARN,
		}),
		&jsonMarshaller,
	)

	_ = jsonMarshaller.Register(eventName, &testStruct{})
	entity := &testStruct{
		ID:   123,
		Name: "John Doe",
	}

	err := publisher.Publish(ctx, testTopic, &pubsub.Message{
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
		sqs:      sqsTest,
		queueURL: queueURL,
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
		ReceiveMessageFunc: func(ctx context.Context, _ *sqs.ReceiveMessageInput, _ ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case r := <-receiveReturns:
				return r.out, r.err
			}
		},
		DeleteMessageBatchFunc: func(_ context.Context, _ *sqs.DeleteMessageBatchInput, _ ...func(*sqs.Options)) (*sqs.DeleteMessageBatchOutput, error) {
			r := <-deleteBatchReturns
			ackOperations <- struct{}{}
			return r.out, r.err
		},
	}

	mc := &Subscriber{
		sqs:      sqsSvc,
		queueURL: "foo",
		ackConfig: AckConfig{
			BatchSize: batchSize,
		},
	}

	input := make([]types.Message, messages)
	for i := 0; i < messages; i++ {
		input[i] = types.Message{
			Body: aws.String("body"),
			MessageAttributes: map[string]types.MessageAttributeValue{
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
			Failed: []types.BatchResultErrorEntry{{
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
		ReceiveMessageFunc: func(ctx context.Context, _ *sqs.ReceiveMessageInput, _ ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
			nextCalls++
			if nextCalls == 1 {
				return &sqs.ReceiveMessageOutput{
					Messages: []types.Message{{
						Body: aws.String("body"),
						MessageAttributes: map[string]types.MessageAttributeValue{
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
		DeleteMessageBatchFunc: func(_ context.Context, _ *sqs.DeleteMessageBatchInput, _ ...func(*sqs.Options)) (*sqs.DeleteMessageBatchOutput, error) {
			close(waitForFlush)
			return nil, errors.New("request failed")
		},
	}

	s := &Subscriber{
		sqs:      sqsSvc,
		queueURL: "foo",
		ackConfig: AckConfig{
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
	_, err := sqsTest.PurgeQueue(ctx, &sqs.PurgeQueueInput{QueueUrl: &queueURL})
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
	ReceiveMessageFunc     func(ctx context.Context, params *sqs.ReceiveMessageInput, optFns ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error)
	DeleteMessageFunc      func(ctx context.Context, params *sqs.DeleteMessageInput, optFns ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error)
	DeleteMessageBatchFunc func(ctx context.Context, params *sqs.DeleteMessageBatchInput, optFns ...func(*sqs.Options)) (*sqs.DeleteMessageBatchOutput, error)
}

func (s *sqsStub) ReceiveMessage(ctx context.Context, params *sqs.ReceiveMessageInput, optFns ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
	return s.ReceiveMessageFunc(ctx, params, optFns...)
}

func (s *sqsStub) DeleteMessageBatch(ctx context.Context, params *sqs.DeleteMessageBatchInput, optFns ...func(*sqs.Options)) (*sqs.DeleteMessageBatchOutput, error) {
	return s.DeleteMessageBatchFunc(ctx, params, optFns...)
}

func (s *sqsStub) DeleteMessage(ctx context.Context, params *sqs.DeleteMessageInput, optFns ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error) {
	return s.DeleteMessageFunc(ctx, params, optFns...)
}

func getEnvOrDefault(key, defaultValue string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultValue
}
