package aws

import (
	"context"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sns"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/hmoragrega/pubsub"
)

var stringDataType = aws.String("String")

type Message struct {
	id         *string
	key        *string
	body       []byte
	attributes map[string]string

	subscriber       *QueueConsumer
	sqsMessageID     *string
	sqsReceiptHandle *string
}

func (m *Message) ID() string {
	return *m.id
}

func (m *Message) Key() string {
	return *m.key
}

func (m *Message) Body() []byte {
	return m.body
}

func (m *Message) Attributes() pubsub.Attributes {
	return m.attributes
}

func (m *Message) Attribute(key string) (string, bool) {
	v, ok := m.attributes[key]
	return v, ok
}

func (m *Message) Ack(ctx context.Context) error {
	return m.subscriber.deleteMessage(ctx, m)
}

func attributesFromSQS(in map[string]*sqs.MessageAttributeValue) (out map[string]string) {
	out = make(map[string]string, len(in))
	for k, v := range in {
		var s string
		switch t := strings.SplitN(*v.DataType, ".", 1)[0]; t {
		case "String":
			s = *v.StringValue
		}
		out[k] = s
	}
	return out
}

func attributesToSNS(in pubsub.Attributes) (out map[string]*sns.MessageAttributeValue) {
	out = make(map[string]*sns.MessageAttributeValue, len(in))
	for k, v := range in {
		out[k] = &sns.MessageAttributeValue{
			DataType:    stringDataType,
			StringValue: &v,
		}
	}
	return out
}
