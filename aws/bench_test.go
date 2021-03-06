//+build integration

package aws

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hmoragrega/pubsub"
	"github.com/hmoragrega/pubsub/marshaller"
)

func TestBench(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	var (
		messagesCount, _ = strconv.Atoi(getEnvOrDefault("BENCH_MESSAGES", "1000"))
		asyncBatch, _    = strconv.Atoi(getEnvOrDefault("BENCH_ASYNC_BATCH", "10"))
		messageSize, _   = strconv.Atoi(getEnvOrDefault("BENCH_MESSAGE_SIZE", "10"))
		topic            = fmt.Sprintf("benchmark-%d", rand.Int31())
		queue            = fmt.Sprintf("%s-queue", topic)
		topicARN         = createTestTopic(ctx, t, topic)
		queueURL         = createTestQueue(ctx, t, queue)
		queueARN         = MustGetResource(GetQueueARN(ctx, sqsTest, queueURL))
		msgMarshaller    = &marshaller.JSONMarshaller{}
	)
	subscribeTestTopic(ctx, t, topicARN, queueARN)
	Must(AttachQueueForwardingPolicy(ctx, sqsTest, queueURL, queueARN, topicARN))

	if err := msgMarshaller.Register(topic, &testBenchStruct{}); err != nil {
		t.Fatal("cannot register type for proto message", err)
	}

	publisher := pubsub.NewPublisher(
		NewSNSPublisher(snsTest, map[string]string{
			topic: topicARN,
		}),
		msgMarshaller,
	)

	if err := publishMessages(t, publisher, topic, messagesCount, messageSize); err != nil {
		t.Fatal("error publishing messages", err)
	}

	subscriber := NewSQSSubscriber(
		sqsTest,
		queueURL,
		WithAck(AckConfig{BatchSize: asyncBatch}),
	)
	counter := NewCounter()

	var wg sync.WaitGroup
	wg.Add(1)

	router := pubsub.Router{
		Unmarshaller: msgMarshaller,
		OnReceive: func(_ context.Context, _ string, _ pubsub.ReceivedMessage, err error) error {
			if err != nil {
				return fmt.Errorf("error receiving message: %v", err)
			}
			return nil
		},
		OnUnmarshal: func(_ context.Context, _ string, _ pubsub.ReceivedMessage, err error) error {
			if err != nil {
				return fmt.Errorf("error unmarshalling message: %v", err)
			}
			return nil
		},
		OnAck: func(_ context.Context, _ string, _ pubsub.ReceivedMessage, err error) error {
			counter.Add(1)
			if counter.Count() == uint64(messagesCount) {
				wg.Done()
			}
			return err
		},
	}
	if err := router.Register(
		topic,
		subscriber,
		pubsub.HandlerFunc(func(ctx context.Context, message *pubsub.Message) error {
			return nil
		}),
	); err != nil {
		panic(err)
	}

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
	t.Logf("consumed %d messages in %s, %f msg/s\n", messagesCount, elapsed, float64(messagesCount)/elapsed.Seconds())

	t.Logf("processed: %d\n", counter.Count())
	t.Logf("mean: %v\n", counter.MeanPerSecond())
	t.Logf("mean throughput: %f\n", counter.MeanPerSecond()/24)

	if err != nil {
		t.Fatal("router finished with error", err)
	}
}

func publishMessages(t *testing.T, publisher pubsub.Publisher, topic string, messagesCount, messageSize int) error {
	rand.Seed(time.Now().UnixNano())

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
				if err := publisher.Publish(context.Background(), topic, msg); err != nil {
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

	t.Logf("sending %d messages\n", messagesCount)

	start := time.Now()

	for ; messagesLeft > 0; messagesLeft-- {
		addMsg <- &pubsub.Message{
			ID:   pubsub.NewID(),
			Name: topic,
			Data: &testBenchStruct{Payload: msgPayload},
		}
	}
	close(addMsg)

	wg.Wait()

	elapsed := time.Now().Sub(start)

	t.Logf("added %d messages in %s, %f msg/s\n", messagesCount, elapsed, float64(messagesCount)/elapsed.Seconds())

	return nil
}

func payload(messageSize int) ([]byte, error) {
	msgPayload := make([]byte, messageSize)
	_, err := rand.Read(msgPayload)
	return msgPayload, err
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

type testBenchStruct struct {
	Payload []byte `json:"payload"`
}
