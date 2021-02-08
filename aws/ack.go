package aws

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/hashicorp/go-multierror"
	"github.com/hmoragrega/workers"
	"github.com/hmoragrega/workers/wrapper"
)

type ackStrategy interface {
	Start() error
	Ack(ctx context.Context, msg *message) error
	Close(ctx context.Context) error
}

type syncAck struct {
	sqs      *sqs.SQS
	queueURL string
}

func newSyncAck(svc *sqs.SQS, queueURL string) *syncAck {
	return &syncAck{
		sqs:      svc,
		queueURL: queueURL,
	}
}

func (s *syncAck) Start() error {
	return nil
}

func (s *syncAck) Ack(ctx context.Context, msg *message) error {
	_, err := s.sqs.DeleteMessageWithContext(ctx, &sqs.DeleteMessageInput{
		ReceiptHandle: msg.sqsReceiptHandle,
		QueueUrl:      &s.queueURL,
	})
	if err != nil {
		return fmt.Errorf("%w: %v", ErrAcknowledgement, err)
	}
	return nil
}

func (s *syncAck) Close(_ context.Context) error {
	return nil
}

type asyncAck struct {
	sqs      *sqs.SQS
	queueURL string
	cfg      AckConfig

	// TODO delete
	counter uint32

	pool       *workers.Pool
	poolConfig workers.Config
	messages   chan *message
	errors     chan error
	pending    sync.WaitGroup
}

func newAsyncAck(svc *sqs.SQS, queueURL string, cfg AckConfig, poolConfig workers.Config) *asyncAck {
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 1
	}
	return &asyncAck{
		sqs:        svc,
		queueURL:   queueURL,
		cfg:        cfg,
		poolConfig: poolConfig,
		messages:   make(chan *message, maxNumberOfMessages),
		errors:     make(chan error, maxNumberOfMessages),
	}
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

func (s *asyncAck) Start() error {
	p, err := workers.NewWithConfig(s.poolConfig)
	if err != nil {
		return fmt.Errorf("cannot create ack pool: %w", err)
	}
	err = p.StartBuilder(s)
	if err != nil {
		return fmt.Errorf("cannot start ack pool: %w", err)
	}
	s.pool = p

	return nil
}

func (s *asyncAck) Close(ctx context.Context) (err error) {
	defer func() {
		println("acked!", atomic.LoadUint32(&s.counter))
	}()

	go func() {
		// wait until all messages have been queue
		s.pending.Wait()
		// close the message so all workers stop.
		close(s.messages)
		// close the pool
		if err := s.pool.Close(ctx); err != nil {
			return
		}
		close(s.errors)
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

func (s *asyncAck) New() workers.Job {
	var (
		size   = s.cfg.BatchSize
		every  = s.cfg.FlushEvery
		batch  = make([]*message, 0, size)
		input  = make([]*sqs.DeleteMessageBatchRequestEntry, 0, size)
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
		input = append(input, &sqs.DeleteMessageBatchRequestEntry{
			Id:            aws.String(m.ID()),
			ReceiptHandle: m.sqsReceiptHandle,
		})
	}

	// flushes the batch.
	flush := func(minimum int) {
		c := len(batch)
		if c < minimum {
			return
		}
		atomic.AddUint32(&s.counter, uint32(c))

		s.batchAck(input, batch)

		// reset the batch
		input = input[:0]
		batch = batch[:0]

		if ticker != nil {
			ticker.Reset(every)
		}
	}

	return wrapper.NoError(func(_ context.Context) {
		for {
			select {
			case <-tick:
				flush(1)
			case m, ok := <-s.messages:
				if !ok {
					// flush one last time
					flush(1)
					// the pool has been closed
					if ticker != nil {
						ticker.Stop()
					}
					return
				}
				add(m)
				flush(size)
			}
		}
	})
}

func (s *asyncAck) batchAck(input []*sqs.DeleteMessageBatchRequestEntry, batch []*message) {
	out, err := s.sqs.DeleteMessageBatch(&sqs.DeleteMessageBatchInput{
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