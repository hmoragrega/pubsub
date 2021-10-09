package storage

import (
	"bytes"
	"encoding/json"

	"github.com/hmoragrega/pubsub"
)

type JSONEnvelopeEncoder struct{}

func (*JSONEnvelopeEncoder) Encode(envelope *pubsub.Envelope) ([]byte, error) {
	var b bytes.Buffer
	err := json.NewEncoder(&b).Encode(envelope)
	if err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

func (*JSONEnvelopeEncoder) Decode(raw []byte) (*pubsub.Envelope, error) {
	var envelope pubsub.Envelope
	err := json.NewDecoder(bytes.NewReader(raw)).Decode(&envelope)
	if err != nil {
		return nil, err
	}
	return &envelope, nil
}
