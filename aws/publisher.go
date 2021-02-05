package aws

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"unsafe"

	"github.com/aws/aws-sdk-go/service/sns"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/hmoragrega/pubsub"
)

var ErrTopicNotFound = errors.New("could not find topic ARN")

const (
	idAttributeKey        = "id"
	versionAttributeKey   = "version"
	keyAttributeKey       = "key"
	nameAttributeKey      = "name"
	customAttributePrefix = "x-custom-"
)

type Publisher struct {
	sns       *sns.SNS
	topicARNs map[string]string
}

func NewPublisher(svc *sns.SNS, topicARNs map[string]string) *Publisher {
	return &Publisher{
		sns:       svc,
		topicARNs: topicARNs,
	}
}

func (p *Publisher) Publish(ctx context.Context, topic string, env pubsub.Envelope) error {
	var topicARN string

	switch pos := strings.Index(topic, "arn:aws:sns"); pos {
	case 0:
		topicARN = topic
	default:
		arn, ok := p.topicARNs[topic]
		if !ok {
			return fmt.Errorf("%w: %s", ErrTopicNotFound, topic)
		}
		topicARN = arn
	}

	// every message needs to have a message group in sns
	key := env.Key
	if key == "" {
		key = "void"
	}

	base64ID := base64.StdEncoding.EncodeToString(env.ID)
	_, err := p.sns.PublishWithContext(ctx, &sns.PublishInput{
		MessageDeduplicationId: &base64ID,
		Message:                stringPtr(env.Body),
		MessageAttributes:      encodeAttributes(&env),
		MessageGroupId:         &key,
		TopicArn:               &topicARN,
	})
	if err != nil {
		return fmt.Errorf("cannot publish message: %w", err)
	}
	return nil
}

func encodeAttributes(env *pubsub.Envelope) map[string]*sns.MessageAttributeValue {
	attributes := map[string]*sns.MessageAttributeValue{
		idAttributeKey: {
			DataType:    binaryDataType,
			BinaryValue: env.ID,
		},
		versionAttributeKey: {
			DataType:    stringDataType,
			StringValue: &env.Version,
		},
		nameAttributeKey: {
			DataType:    stringDataType,
			StringValue: &env.Name,
		},
	}
	if env.Key != "" {
		attributes[keyAttributeKey] = &sns.MessageAttributeValue{
			DataType:    stringDataType,
			StringValue: &env.Key,
		}
	}
	for k, v := range env.Attributes {
		v := v
		k := customAttributePrefix + k
		attributes[k] = &sns.MessageAttributeValue{
			DataType:    stringDataType,
			StringValue: &v,
		}
	}
	return attributes
}

func decodeAttributes(attributes map[string]*sqs.MessageAttributeValue) map[string]string {
	custom := make(map[string]string)
	for k, v := range attributes {
		if strings.Index(k, customAttributePrefix) != 0 {
			continue
		}
		k := k[len(customAttributePrefix):]
		var value string
		if v.StringValue != nil {
			value = *v.StringValue
		}
		custom[k] = value
	}
	return custom
}

func stringPtr(b []byte) *string {
	sh := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	return (*string)(unsafe.Pointer(&reflect.StringHeader{
		Data: sh.Data,
		Len:  sh.Len,
	}))
}
