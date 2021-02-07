package aws

import (
	"context"
	"encoding/base64"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sns"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/hmoragrega/pubsub"
	"github.com/hmoragrega/workers"
)

const (
	messagesCount = 10000
)

var sqsTest *sqs.SQS
var snsTest *sns.SNS

func Bench() {
	var sess *session.Session
	var err error
	if os.Getenv("AWS") == "true" {
		sess, err = session.NewSession(&aws.Config{
			Region: aws.String("eu-west-3"),
			//Logger:   aws.NewDefaultLogger(),
			//LogLevel: aws.LogLevel(aws.LogDebug | aws.LogDebugWithHTTPBody),
		})
	} else {
		sess, err = session.NewSessionWithOptions(session.Options{
			Config: aws.Config{
				Credentials: credentials.NewStaticCredentials("id", "secret", "token"),
				Endpoint:    aws.String(getEnvOrDefault("AWS_ENDPOINT", "localhost:4100")), //goaws
				//Endpoint:    aws.String(getEnvOrDefault("AWS_ENDPOINT", "localhost:4566")), // localstack
				Region:     aws.String(getEnvOrDefault("AWS_REGION", "eu-west-3")),
				DisableSSL: aws.Bool(true),
				//Logger:     aws.NewDefaultLogger(),
				//LogLevel:   aws.LogLevel(aws.LogDebug | aws.LogDebugWithHTTPBody),
			},
		})
	}
	if err != nil {
		panic(err)
	}
	sqsTest = sqs.New(sess)
	snsTest = sns.New(sess)

	ctx := context.Background()

	topic := "benchmark"
	topicARN := CreateTestTopic(ctx, topic)
	queueURL, queueARN := CreateTestQueue(ctx, "my-queue")
	SubscribeTestTopic(ctx, topicARN, queueARN)

	unmarshaler := &noOpMarshaler{}
	publisher := &pubsub.Publisher{
		Publisher: &Publisher{
			SNS: snsTest,
			TopicARNs: map[string]string{
				topic: topicARN,
			},
		},
		Marshaler: unmarshaler,
	}

	if err := publishMessages(publisher, topic, 10); err != nil {
		panic(err)
	}

	subscriber := &Subscriber{
		SQS:      sqsTest,
		QueueURL: queueURL,
		AckConfig: AckConfig{
			Timeout:    0,
			BatchSize:  10,
			FlushEvery: 0,
		},
		WorkersConfig: workers.Config{
			Initial: 8,
		},
	}

	counter := NewCounter()

	var wg sync.WaitGroup
	wg.Add(1)

	router := pubsub.Router{
		Unmarshaler:    unmarshaler,
		DisableAutoAck: false,
		StopTimeout:    0,
		//		OnReceive: func(topic string, msg pubsub.ReceivedMessage, err error) error {
		//			fmt.Println("on receive", topic, msg.ID(), err)
		//			return nil
		//		},
		//		OnUnmarshal: func(topic string, msg pubsub.ReceivedMessage, err error) error {
		//			fmt.Println("on unmarshal", topic, msg.ID(), err)
		//			return nil
		//		},
		//		OnHandler: func(topic string, msg pubsub.ReceivedMessage, err error) error {
		//			fmt.Println("on handler", topic, msg.ID(), err)
		//			return nil
		//		},
		OnAck: func(_ context.Context, _ string, _ pubsub.ReceivedMessage, _ error) error {
			//fmt.Println("on ack", topic, msg.ID(), err)
			counter.Add(1)
			if counter.Count() == messagesCount {
				wg.Done()
			}
			return nil
		},
	}
	err = router.RegisterHandler(
		topic,
		subscriber,
		pubsub.MessageHandlerFunc(func(ctx context.Context, message *pubsub.Message) error {
			return nil
		}),
	)
	if err != nil {
		panic(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		wg.Wait()
		cancel()
	}()

	go func() {
		ticker := time.NewTicker(time.Second * 5)
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				fmt.Printf("processed: %d\n", counter.Count())
			}
		}
	}()

	start := time.Now()
	err = router.Run(ctx)

	elapsed := time.Now().Sub(start)
	fmt.Printf("consumed %d messages in %s, %f msg/s\n", messagesCount, elapsed, float64(messagesCount)/elapsed.Seconds())

	fmt.Printf("processed: %d\n", counter.Count())
	fmt.Printf("mean: %v\n", counter.MeanPerSecond())
	fmt.Printf("mean throughput: %f\n", counter.MeanPerSecond()/24)

	if err != nil {
		panic(err)
	}
}

type noOpMarshaler struct{}

func (m *noOpMarshaler) Marshal(data interface{}) ([]byte, error) {
	return []byte(data.(string)), nil
}

func (m *noOpMarshaler) Version() string {
	return "no-op"
}

func (m *noOpMarshaler) Unmarshal(message pubsub.ReceivedMessage) (*pubsub.Message, error) {
	return &pubsub.Message{
		ID:   message.ID(),
		Name: message.Name(),
		Key:  message.Key(),
		Data: message.Body(),
	}, nil
}

func publishMessages(publisher *pubsub.Publisher, topic string, messageSize int) error {
	rand.Seed(time.Now().UnixNano())

	fmt.Printf("sending %d messages\n", messagesCount)

	messagesLeft := messagesCount
	works := 64

	wg := sync.WaitGroup{}
	wg.Add(works)

	addMsg := make(chan *pubsub.Message)

	var count int32
	for num := 0; num < works; num++ {
		go func() {
			defer wg.Done()

			for msg := range addMsg {
				if err := publisher.Publish(context.Background(), topic, *msg); err != nil {
					panic(err)
				}
				atomic.AddInt32(&count, 1)
			}
		}()
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		ticker := time.NewTicker(time.Second)
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				fmt.Printf("sent: %d\n", atomic.LoadInt32(&count))
			}
		}
	}()

	msgPayload, err := payload(messageSize)
	if err != nil {
		return err
	}

	start := time.Now()

	for ; messagesLeft > 0; messagesLeft-- {
		addMsg <- &pubsub.Message{
			ID:   pubsub.NewID(),
			Name: topic,
			Data: msgPayload,
		}
	}
	close(addMsg)

	wg.Wait()

	elapsed := time.Now().Sub(start)

	fmt.Printf("added %d messages in %s, %f msg/s\n", messagesCount, elapsed, float64(messagesCount)/elapsed.Seconds())

	return nil
}

func CreateTestQueue(ctx context.Context, queueName string) (string, string) {
	queueURL := MustCreateResource(CreateQueue(ctx, sqsTest, queueName))
	/*	_, err := sqsTest.PurgeQueueWithContext(ctx, &sqs.PurgeQueueInput{QueueUrl: &queueURL})
		if err != nil {
			panic("cannot purge queue")
		}
	*/
	return queueURL, MustCreateResource(GetQueueARN(ctx, sqsTest, queueURL))
}

func SubscribeTestTopic(ctx context.Context, topicARN, queueARN string) string {
	subscriptionARN := MustCreateResource(Subscribe(ctx, snsTest, topicARN, queueARN))
	return subscriptionARN
}

func CreateTestTopic(ctx context.Context, topicName string) string {
	queueURL := MustCreateResource(CreateTopic(ctx, snsTest, topicName))
	return queueURL
}

func getEnvOrDefault(key string, d string) string {
	s := os.Getenv(key)
	if s == "" {
		return d
	}
	return s
}

func payload(messageSize int) (string, error) {
	msgPayload := make([]byte, messageSize)
	_, err := rand.Read(msgPayload)
	if err != nil {
		panic(err)
	}

	return base64.StdEncoding.EncodeToString(msgPayload), nil
}

type Counter struct {
	count     uint64
	startTime time.Time
}

func NewCounter() *Counter {
	return &Counter{
		count:     0,
		startTime: time.Now(),
	}
}

func (c *Counter) Add(n uint64) {
	atomic.AddUint64(&c.count, n)
}

func (c *Counter) Count() uint64 {
	return atomic.LoadUint64(&c.count)
}

func (c *Counter) MeanPerSecond() float64 {
	return float64(c.count) / time.Since(c.startTime).Seconds()
}
