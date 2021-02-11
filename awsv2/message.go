package aws

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
)

var (
	stringDataType = aws.String("String")
	binaryDataType = aws.String("Binary")
)

type message struct {
	id         *string
	key        *string
	name       *string
	version    *string
	body       []byte
	attributes map[string]string

	subscriber       *Subscriber
	sqsMessageID     *string
	sqsReceiptHandle *string
}

func (m *message) ID() string {
	return *m.id
}

func (m *message) Key() string {
	return *m.key
}

func (m *message) Body() []byte {
	return m.body
}

func (m *message) Name() string {
	return *m.name
}

func (m *message) Version() string {
	return *m.version
}

func (m *message) Attributes() map[string]string {
	return m.attributes
}

func (m *message) Ack(ctx context.Context) error {
	return m.subscriber.ack(ctx, m)
}
