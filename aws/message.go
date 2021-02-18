package aws

import (
	"context"
	"fmt"

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
	return m.subscriber.ack(ctx, m)
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
