package aws

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/aws/aws-sdk-go-v2/aws"
)

var stringDataType = aws.String("String")

type message struct {
	id         string
	key        string
	name       string
	version    string
	body       string
	attributes map[string]string

	ackNotifications chan<- struct{}
	acknowledged     int32
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
	if atomic.SwapInt32(&m.acknowledged, 1) == 0 {
		m.ackNotifications <- struct{}{}
		return m.subscriber.ack(ctx, m)
	}
	return nil
}

func (m *message) NAck(ctx context.Context) error {
	if atomic.SwapInt32(&m.acknowledged, 1) == 0 {
		m.ackNotifications <- struct{}{}
		return m.subscriber.nack(ctx, m)
	}
	return nil
}

func (m *message) String() string {
	return fmt.Sprintf(
		"{id: %s, key: %s, name: %s, version: %s, body: %s, attributtes: %+v}",
		m.id,
		m.key,
		m.name,
		m.version,
		m.body,
		m.attributes,
	)
}
