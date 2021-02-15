package marshaller

import (
	"errors"

	"github.com/hashicorp/go-multierror"
	"github.com/hmoragrega/pubsub"
)

var ErrEmptyUnmarshallerChain = errors.New("empty unmarshaller chain")

type ChainUnmarshaller struct {
	unmarshallers []pubsub.Unmarshaller
}

func NewChainUnmarshaller(unmarshallers ...pubsub.Unmarshaller) *ChainUnmarshaller {
	return &ChainUnmarshaller{unmarshallers: unmarshallers}
}

func (c *ChainUnmarshaller) Unmarshal(topic string, message pubsub.ReceivedMessage) (*pubsub.Message, error) {
	var mErr error
	if len(c.unmarshallers) == 0 {
		return nil, ErrEmptyUnmarshallerChain
	}

	for _, um := range c.unmarshallers {
		msg, err := um.Unmarshal(topic, message)
		if err != nil {
			mErr = multierror.Append(mErr, err)
			continue
		}

		return msg, nil
	}

	return nil, mErr
}
