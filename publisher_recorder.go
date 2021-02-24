package pubsub

import (
	"context"
	"sync"
)

// MessageRecord record the message and the topic.
type MessageRecord struct {
	Topic   string
	Message *Message
}

// PublisherRecorder is a publisher middleware that records
// all the events that are published.
type PublisherRecorder struct {
	publisher     Publisher
	messages      []*MessageRecord
	topicMessages map[string][]*Message
	mx            sync.RWMutex
}

// NewPublisherRecorder returns a new publisher recorder.
func NewPublisherRecorder(publisher Publisher) *PublisherRecorder {
	return &PublisherRecorder{
		publisher:     publisher,
		topicMessages: make(map[string][]*Message),
	}
}

func (p *PublisherRecorder) Publish(ctx context.Context, topic string, messages ...*Message) error {
	p.mx.Lock()
	if p.topicMessages == nil {
		p.topicMessages = make(map[string][]*Message)
	}
	for _, message := range messages {
		p.messages = append(p.messages, &MessageRecord{Topic: topic, Message: message})
		p.topicMessages[topic] = append(p.topicMessages[topic], message)
	}
	p.mx.Unlock()

	return p.publisher.Publish(ctx, topic, messages...)
}

// Messages returns a list of records in the same order that have been published.
func (p *PublisherRecorder) Messages() []*MessageRecord {
	p.mx.RLock()
	defer p.mx.RUnlock()

	return p.messages
}

// MessagesMap returns a map of all the messages published by topic.
func (p *PublisherRecorder) MessagesMap() map[string][]*Message {
	p.mx.RLock()
	defer p.mx.RUnlock()

	return p.topicMessages
}

// TopicMessages returns list of message sent in a single topic.
func (p *PublisherRecorder) TopicMessages(topic string) []*Message {
	p.mx.RLock()
	defer p.mx.RUnlock()

	return p.topicMessages[topic]
}

// Reset empties the lists of published messages.
func (p *PublisherRecorder) Reset() {
	p.mx.Lock()
	defer p.mx.Unlock()

	p.messages = nil
	p.topicMessages = nil
}
