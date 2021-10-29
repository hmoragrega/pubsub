package aws

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"unsafe"

	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/hmoragrega/pubsub"
)

var ErrTopicNotFound = errors.New("could not find topic ARN")

// SNSPublisher SNS publisher.
type SNSPublisher struct {
	sns       *sns.Client
	topicARNs map[string]string
}

// NewSNSPublisher creates a new SNS publisher.
func NewSNSPublisher(sns *sns.Client, topicARNs map[string]string) *SNSPublisher {
	return &SNSPublisher{
		sns:       sns,
		topicARNs: topicARNs,
	}
}

// Publish a message trough SNS.
func (p *SNSPublisher) Publish(ctx context.Context, topic string, envelopes ...*pubsub.Envelope) error {
	var topicARN string

	if isSNSTopicARN(topic) {
		topicARN = topic
	} else {
		arn, ok := p.topicARNs[topic]
		if !ok {
			return fmt.Errorf("%w: %s", ErrTopicNotFound, topic)
		}
		topicARN = arn
	}

	return publishSNSMessage(ctx, p.sns, topicARN, envelopes...)
}

func isSNSTopicARN(s string) bool {
	return strings.HasPrefix(s, "arn:aws:sns")
}

func stringPtr(b []byte) *string {
	sh := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	return (*string)(unsafe.Pointer(&reflect.StringHeader{
		Data: sh.Data,
		Len:  sh.Len,
	}))
}

func byteFromStringPtr(s *string) []byte {
	sh := (*reflect.StringHeader)(unsafe.Pointer(s))
	return *(*[]byte)(unsafe.Pointer(&reflect.SliceHeader{
		Data: sh.Data,
		Len:  sh.Len,
		Cap:  sh.Len,
	}))
}
