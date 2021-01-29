package pubsub

import (
	"context"
	"errors"
)

var (
	// ErrEmptyMessageID is triggered when the message does not have an ID.
	ErrEmptyMessageID = errors.New("empty message ID")
	// ErrEmptyMessageBody is triggered when the message does not have body.
	ErrEmptyMessageBody = errors.New("empty message body")
)

// ConsumeResult is the result of consuming
// messages from the queue.
//
// It contains a list of consumed message
// or an error if the operation failed.
type ConsumeResult struct {
	Message Message
	Err     error
}

// Consumer can consume messages
// from a pub/sub system and return them.
type Consumer interface {
	// Consume the next message in the queue. If there is no message
	// available it has to block until a new message is received or the
	// context expires
	Consume() <-chan ConsumeResult
}

type Subscriber struct {
	Consumer Consumer
}

func (s *Subscriber) Next(ctx context.Context) (Message, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case result := <-s.Consumer.Consume():
		return result.Message, result.Err
	}
}
