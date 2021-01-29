package aws

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"unsafe"

	"github.com/aws/aws-sdk-go/service/sns"
	"github.com/hmoragrega/pubsub"
)

var (
	ErrEmptyTopicsMap = errors.New("empty topics map")
	ErrTopicNotFound  = errors.New("could not find topic ARN")
)

type Publisher struct {
	sns    *sns.SNS
	topics map[string]string
	mx     sync.RWMutex
}

type PublisherOption func(*Publisher)

func WithPublisherSNS(svc *sns.SNS) PublisherOption {
	return func(s *Publisher) {
		s.sns = svc
	}
}

func NewPublisher(topics map[string]string, opts ...PublisherOption) (*Publisher, error) {
	if len(topics) == 0 {
		return nil, ErrEmptyTopicsMap
	}
	p := &Publisher{
		topics: topics,
	}
	for _, opt := range opts {
		opt(p)
	}
	if p.sns == nil {
		svc, err := NewDefaultSNS()
		if err != nil {
			return nil, err
		}
		p.sns = svc
	}

	return p, nil
}

func MustPublisher(publisher *Publisher, err error) *Publisher {
	if err != nil {
		panic(err)
	}
	return publisher
}

func (p *Publisher) Publish(ctx context.Context, topic string, env pubsub.Envelope) error {
	topicARN, ok := p.topics[topic]
	if !ok {
		return ErrTopicNotFound
	}

	_, err := p.sns.PublishWithContext(ctx, &sns.PublishInput{
		Message:                stringPtr(env.Body),
		MessageAttributes:      attributesToSNS(env.Attributes),
		MessageDeduplicationId: stringPtr(env.ID),
		MessageGroupId:         &env.Key,
		TopicArn:               &topicARN,
	})
	if err != nil {
		return fmt.Errorf("cannot publish message: %w", err)
	}
	return nil
}

func (p *Publisher) topicARN(topic string) (string, error) {
	p.mx.RLock()
	defer p.mx.RUnlock()

	topicARN, ok := p.topics[topic]
	if !ok {
		return "", ErrTopicNotFound
	}

	return topicARN, nil
}

// stringPtr unsafe copy-free byte slice to string pointer.
func stringPtr(b []byte) *string {
	sh := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	return (*string)(unsafe.Pointer(&reflect.StringHeader{
		Data: sh.Data,
		Len:  sh.Len,
	}))
}
