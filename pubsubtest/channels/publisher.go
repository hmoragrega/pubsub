package channels

import (
	"context"
	"sync"

	"github.com/hmoragrega/pubsub"
	"github.com/hmoragrega/pubsub/pubsubtest/stubs"
)

const queueBuffer = 100

type Publisher struct {
	subscribers map[string]map[string]*Subscriber
	mx          sync.RWMutex
}

func (p *Publisher) Publish(ctx context.Context, topic string, envelopes ...*pubsub.Envelope) error {
	p.mx.RLock()
	defer p.mx.RUnlock()

	for _, s := range p.subscribers[topic] {
		for _, envelope := range envelopes {
			e := envelope
			rm := stubs.NewNoOpReceivedMessage()
			rm.IDFunc = func() string {
				return e.ID
			}
			rm.NameFunc = func() string {
				return e.Name
			}
			rm.KeyFunc = func() string {
				return e.Key
			}
			rm.BodyFunc = func() []byte {
				return e.Body
			}
			rm.VersionFunc = func() string {
				return e.Version
			}
			rm.AttributesFunc = func() pubsub.Attributes {
				return e.Attributes
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case s.next <- pubsub.Next{Message: rm}:
			}
		}
	}

	return nil
}

func (p *Publisher) Subscriber(topic string) *Subscriber {
	p.mx.Lock()
	defer p.mx.Unlock()

	id := pubsub.NewID()

	s := &Subscriber{
		next: make(chan pubsub.Next, queueBuffer),
		stop: func() {
			p.mx.Lock()
			defer p.mx.Unlock()
			delete(p.subscribers[topic], id)
		},
	}

	if p.subscribers == nil {
		p.subscribers = make(map[string]map[string]*Subscriber)
	}

	m, ok := p.subscribers[topic]
	if !ok {
		p.subscribers[topic] = make(map[string]*Subscriber)
		m = p.subscribers[topic]
	}

	m[id] = s

	return s
}

type Subscriber struct {
	id   string
	next chan pubsub.Next
	stop func()
}

func (s *Subscriber) Subscribe() (<-chan pubsub.Next, error) {
	return s.next, nil
}

func (s *Subscriber) Stop(_ context.Context) error {
	s.stop()
	return nil
}
