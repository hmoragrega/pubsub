package stubs

import (
	"context"

	"github.com/hmoragrega/pubsub"
)

type ReceivedMessageStub struct {
	IDFunc         func() string
	NameFunc       func() string
	KeyFunc        func() string
	BodyFunc       func() []byte
	VersionFunc    func() string
	AttributesFunc func() pubsub.Attributes
	AckFunc        func(ctx context.Context) error
	StringFunc     func() string
}

func NewNoOpReceivedMessage() *ReceivedMessageStub {
	return &ReceivedMessageStub{
		IDFunc: func() string {
			return ""
		},
		NameFunc: func() string {
			return ""
		},
		KeyFunc: func() string {
			return ""
		},
		BodyFunc: func() []byte {
			return nil
		},
		VersionFunc: func() string {
			return ""
		},
		AttributesFunc: func() pubsub.Attributes {
			return nil
		},
		AckFunc: func(ctx context.Context) error {
			return nil
		},
		StringFunc: func() string {
			return ""
		},
	}
}

func (m *ReceivedMessageStub) ID() string {
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

func (m *ReceivedMessageStub) String() string {
	return m.StringFunc()
}
