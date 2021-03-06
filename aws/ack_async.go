package aws

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/hashicorp/go-multierror"
)

var ErrAsyncNAckNotSupported = errors.New("NAck on async strategy is not supported")

type asyncAck struct {
	sqs      sqsSvc
	queueURL string
	cfg      AckConfig

	messages chan *message
	errors   chan error
	pending  sync.WaitGroup
}

func newAsyncAck(svc sqsSvc, queueURL string, cfg AckConfig) *asyncAck {
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 1
	}
	s := &asyncAck{
		sqs:      svc,
		queueURL: queueURL,
		cfg:      cfg,
		messages: make(chan *message, maxNumberOfMessages),
		errors:   make(chan error, maxNumberOfMessages),
	}
	s.run()
	return s
}

func (s *asyncAck) Ack(_ context.Context, msg *message) error {
	s.pending.Add(1)
	go func() {
		s.messages <- msg
		s.pending.Done()
	}()

	// Return previous errors.
	select {
	case err := <-s.errors:
		return err
	default:
		return nil
	}
}

func (s *asyncAck) NAck(_ context.Context, _ *message) error {
	return nil
}

func (s *asyncAck) run() {
	var (
		size   = s.cfg.BatchSize
		every  = s.cfg.FlushEvery
		batch  = make([]*message, 0, size)
		ticker *time.Ticker
		tick   <-chan time.Time
	)

	if every > 0 {
		ticker = time.NewTicker(every)
		tick = ticker.C
	}

	// adds a message to the batch.
	add := func(m *message) {
		batch = append(batch, m)
	}

	// flushes the batch.
	flush := func(minimum int) {
		c := len(batch)
		if c < minimum {
			return
		}
		s.batchAck(batch)

		// reset the batch
		batch = batch[:0]

		if ticker != nil {
			ticker.Reset(every)
		}
	}

	go func() {
		defer close(s.errors)
		for {
			select {
			case <-tick:
				flush(1)
			case m, ok := <-s.messages:
				if !ok {
					// flush one last time
					flush(1)
					// the pool has been stopped
					if ticker != nil {
						ticker.Stop()
					}
					return
				}
				add(m)
				flush(size)
			}
		}
	}()
}

func (s *asyncAck) Close(ctx context.Context) (err error) {
	go func() {
		// wait until all messages have been queued
		s.pending.Wait()
		// close the message so all workers stop eventually
		close(s.messages)
	}()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case ackError, ok := <-s.errors:
			if !ok {
				return err
			}
			err = multierror.Append(err, ackError)
		}
	}
}

func (s *asyncAck) batchAck(batch []*message) {
	input := make([]types.DeleteMessageBatchRequestEntry, len(batch))
	for i, m := range batch {
		input[i] = types.DeleteMessageBatchRequestEntry{
			Id:            aws.String(m.ID()),
			ReceiptHandle: m.sqsReceiptHandle,
		}
	}
	out, err := s.sqs.DeleteMessageBatch(context.Background(), &sqs.DeleteMessageBatchInput{
		Entries:  input,
		QueueUrl: &s.queueURL,
	})

	if err != nil {
		var ackErr error
		for _, msg := range batch {
			ackErr = multierror.Append(ackErr, fmt.Errorf("%w %s: %v", ErrAcknowledgement, msg.ID(), err))
		}
		s.errors <- ackErr
		return
	}

	if len(out.Failed) > 0 {
		var ackErr error
		for _, msg := range out.Failed {
			ackErr = multierror.Append(ackErr, fmt.Errorf("%w %s: %s", ErrAcknowledgement, *msg.Id, *msg.Code))
		}
		s.errors <- ackErr
	}
}
