package pulsar

import (
	"context"

	"github.com/apache/pulsar-client-go/pulsar"
)

type message struct {
	id         []byte
	key        string
	name       string
	version    string
	body       []byte
	attributes map[string]string

	consumer      pulsar.Consumer
	pulsarMessage pulsar.Message
}

func (m *message) ID() []byte {
	return m.id
}

func (m *message) Key() string {
	return m.key
}

func (m *message) Body() []byte {
	return m.body
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
	ok := make(chan struct{})
	go func() {
		// TODO again a blocking call...
		m.consumer.Ack(m.pulsarMessage)
		close(ok)
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-ok:
		return nil
	}
}
