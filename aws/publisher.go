package aws

import (
	"context"
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
	SNS       *sns.SNS
	TopicARNs map[string]string
}

func (p *Publisher) Publish(ctx context.Context, topic string, env pubsub.Envelope) error {
	var topicARN string

	switch pos := strings.Index(strings.ToLower(topic), "arn:aws:sns"); pos {
	case 0:
		topicARN = topic
	default:
		arn, ok := p.TopicARNs[topic]
		if !ok {
			return fmt.Errorf("%w: %s", ErrTopicNotFound, topic)
		}
		topicARN = arn
	}

	// every FIFO queue message needs to have a message group in SNS
	// @TODO only for FIFO
	// key := env.Key
	// if key == "" {
	//		key = "void"
	//	}

	_, err := p.SNS.PublishWithContext(ctx, &sns.PublishInput{
		TopicArn:          &topicARN,
		Message:           stringPtr(env.Body),
		MessageAttributes: encodeAttributes(&env),
		//MessageDeduplicationId: &base64ID, // @TODO FIFO only
		//MessageGroupId:         &key,      // @TODO FIFO only
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
