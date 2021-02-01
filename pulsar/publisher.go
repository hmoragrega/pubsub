package pulsar

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/hmoragrega/pubsub"
)

var errProducerNotFound = errors.New("no producer for topic")

const (
	idAttributeKey        = "id"
	versionAttributeKey   = "version"
	keyAttributeKey       = "key"
	nameAttributeKey      = "name"
	customAttributePrefix = "x-custom-"
)

type Publisher struct {
	Producers map[string]pulsar.Producer
}

func (p *Publisher) Publish(ctx context.Context, topic string, env pubsub.Envelope) error {
	producer, ok := p.Producers[topic]
	if !ok {
		return fmt.Errorf("%w: %s", errProducerNotFound, topic)
	}

	// pulsar library blocks for real if the events channel is full for the producer.
	result := make(chan error, 1)
	go func() {
		_, err := producer.Send(ctx, &pulsar.ProducerMessage{
			Payload:    env.Body,
			Key:        env.Key,
			Properties: encodeAttributes(&env),
			//EventTime:         time.Time{}, // TODO support event time in envelope
		})

		result <- err
		close(result)
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-result:
		return err
	}
}

func encodeAttributes(env *pubsub.Envelope) map[string]string {
	attributes := map[string]string{
		idAttributeKey:      base64.StdEncoding.EncodeToString(env.ID),
		versionAttributeKey: env.Version,
		nameAttributeKey:    env.Name,
	}
	if env.Key != "" {
		attributes[keyAttributeKey] = env.Key
	}
	for k, v := range env.Attributes {
		k := customAttributePrefix + k
		attributes[k] = v
	}
	return attributes
}

func decodeAttributes(attributes map[string]string) map[string]string {
	custom := make(map[string]string)
	for k, v := range attributes {
		if strings.Index(k, customAttributePrefix) != 0 {
			continue
		}
		k := k[len(customAttributePrefix):]
		custom[k] = v
	}
	return custom
}
