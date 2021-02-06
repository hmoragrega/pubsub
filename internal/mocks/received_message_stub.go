package mocks

import (
	"context"

	"github.com/hmoragrega/pubsub"
)

type ReceivedMessageStub struct {
	IDFunc         func() []byte
	NameFunc       func() string
	KeyFunc        func() string
	BodyFunc       func() []byte
	VersionFunc    func() string
	AttributesFunc func() pubsub.Attributes
	AckFunc        func(ctx context.Context) error
}

func (m *ReceivedMessageStub) ID() []byte {
	return m.IDFunc()
}

func (m *ReceivedMessageStub) Name() string {
	return m.NameFunc()
}

func (m *ReceivedMessageStub) Key() string {
	return m.KeyFunc()
}

func (m *ReceivedMessageStub) Body() []byte {
	return m.BodyFunc()
}

func (m *ReceivedMessageStub) Version() string {
	return m.VersionFunc()
}

func (m *ReceivedMessageStub) Attributes() pubsub.Attributes {
	return m.AttributesFunc()
}

func (m *ReceivedMessageStub) Ack(ctx context.Context) error {
	return m.AckFunc(ctx)
}
