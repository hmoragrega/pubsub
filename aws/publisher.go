package aws

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"unsafe"

	"github.com/aws/aws-sdk-go/service/sns"
	"github.com/hmoragrega/pubsub"
)

var ErrTopicNotFound = errors.New("could not find topic ARN")

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

func stringPtr(b []byte) *string {
	sh := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	return (*string)(unsafe.Pointer(&reflect.StringHeader{
		Data: sh.Data,
		Len:  sh.Len,
	}))
}
