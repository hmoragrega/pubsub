package aws

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
)

var stringDataType = aws.String("String")

type message struct {
	id           string
	key          string
	name         string
	version      string
	body         string
	receiveCount int
	attributes   map[string]string

	ackNotifications chan<- struct{}
	ackOnce          sync.Once
	ackResult        error

	subscriber       *Subscriber
	sqsMessageID     *string
	sqsReceiptHandle *string
}

func (m *message) ID() string {
	return m.id
}

func (m *message) Key() string {
	return m.key
}

func (m *message) Body() []byte {
	return byteFromStringPtr(&m.body)
}

func (m *message) Name() string {
	return m.name
}

func (m *message) Version() string {
	return m.version
}

func (m *message) Attributes() map[string]string {
	return m.attributes
}

func (m *message) Ack(ctx context.Context) error {
	m.ackOnce.Do(func() {
		m.ackNotifications <- struct{}{}
		m.ackResult = m.subscriber.ack(ctx, m)
	})

	return m.ackResult
}

func (m *message) NAck(ctx context.Context) error {
	m.ackOnce.Do(func() {
		m.ackNotifications <- struct{}{}
		m.ackResult = m.subscriber.nack(ctx, m)
	})

	return m.ackResult
}

func (m *message) ReSchedule(ctx context.Context, delay time.Duration) error {
	m.ackOnce.Do(func() {
		m.ackNotifications <- struct{}{}
		m.ackResult = m.subscriber.changeVisibility(ctx, m, delay)
	})

	return m.ackResult
}

func (m *message) ReceivedCount() int {
	return m.receiveCount
}

func (m *message) String() string {
	return fmt.Sprintf(
		"{id: %s, key: %s, name: %s, version: %s, body: %s, receive_count: %d, attributtes: %+v; }",
		m.id,
		m.key,
		m.name,
		m.version,
		m.body,
		m.receiveCount,
		m.attributes,
	)
}
