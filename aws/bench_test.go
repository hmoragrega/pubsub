//+build integration

package aws

import (
	"context"
	"encoding/base64"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hmoragrega/pubsub"
	"github.com/hmoragrega/workers"
)

const (
	messagesCount = 10000
)

func TestBench(t *testing.T) {
	ctx := context.Background()

	var (
		topic        = fmt.Sprintf("benchmark-%d", rand.Int31())
		queue        = fmt.Sprintf("%s-queue", topic)
		topicARN     = MustCreateResource(CreateTopic(ctx, snsTest, topic))
		queueURL     = MustCreateResource(CreateQueue(ctx, sqsTest, queue))
		queueARN     = MustCreateResource(GetQueueARN(ctx, sqsTest, queueURL))
		subscription = MustCreateResource(Subscribe(ctx, snsTest, topicARN, queueARN))
		marshaller   = &pubsub.NoOpMarshaller{}
	)
	t.Cleanup(func() {
		ctx := context.Background()
		Must(Unsubscribe(ctx, snsTest, subscription))
		Must(DeleteQueue(ctx, sqsTest, queueURL))
		Must(DeleteTopic(ctx, snsTest, topicARN))
	})

	publisher := &pubsub.Publisher{
		Publisher: &Publisher{
			SNS: snsTest,
			TopicARNs: map[string]string{
				topic: topicARN,
			},
		},
		Marshaler: marshaller,
	}

	if err := publishMessages(publisher, topic, 10); err != nil {
		t.Fatal("error publishing messages", err)
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
		Unmarshaler: marshaller,
		OnAck: func(_ context.Context, _ string, _ pubsub.ReceivedMessage, err error) error {
			counter.Add(1)
			if counter.Count() == messagesCount {
				wg.Done()
			}
			return nil
		},
	}
	if err := router.RegisterHandler(
		topic,
		subscriber,
		pubsub.MessageHandlerFunc(func(ctx context.Context, message *pubsub.Message) error {
			return nil
		}),
	); err != nil {
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
	err := router.Run(ctx)

	elapsed := time.Now().Sub(start)
	fmt.Printf("consumed %d messages in %s, %f msg/s\n", messagesCount, elapsed, float64(messagesCount)/elapsed.Seconds())

	fmt.Printf("processed: %d\n", counter.Count())
	fmt.Printf("mean: %v\n", counter.MeanPerSecond())
	fmt.Printf("mean throughput: %f\n", counter.MeanPerSecond()/24)

	if err != nil {
		t.Fatal("router finished with error", err)
	}
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
