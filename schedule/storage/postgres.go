package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/hmoragrega/pubsub"
)

type executor interface {
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
}

type encoder interface {
	Encode(envelope *pubsub.Envelope) ([]byte, error)
	Decode(raw []byte) (*pubsub.Envelope, error)
}

type Options struct {
	// MinWaitTime minimum wait time between that will wait before processing the next batch of due messages.
	// If the batch took more to be processed, the next one will be processed immediately.
	// On mostly empty tables a longer time will prevent hitting the DB constantly
	// at the cost of losing some precision in the due date.
	// Default: 500ms.
	MinWaitTime time.Duration

	// BatchSize number of messages that will be processed at the same time.
	// Default: 100.
	BatchSize int

	// InstanceTimeout specifies the time that a message will be locked for publishing after
	// has been selected for this instance. After this time, the message will be available again.
	// A reasonable value should allow you app to publish all the messages in the batch.
	// Default: 1 minute.
	InstanceTimeout time.Duration

	// Encoder can be used to customize how to encode the messages for storing.
	// Default: JSON encoder
	Encoder encoder
}

var _ pubsub.SchedulerStorage = &Postgres{}

type Postgres struct {
	instanceID string
	exec       executor
	opts       Options
	encoder    encoder

	insertQuery string
	selectQuery string
	deleteQuery string
}

func NewPostgres(instanceID, table string, exec executor) *Postgres {
	return NewPostgresWithOptions(instanceID, table, exec, Options{})
}

func NewPostgresWithOptions(instanceID, table string, exec executor, opts Options) *Postgres {
	if opts.MinWaitTime == 0 {
		opts.MinWaitTime = 500 * time.Millisecond
	}
	if opts.BatchSize <= 0 {
		opts.BatchSize = 100
	}
	if opts.InstanceTimeout <= 0 {
		opts.InstanceTimeout = time.Minute
	}
	encoder := opts.Encoder
	if encoder == nil {
		encoder = &JSONEnvelopeEncoder{}
	}
	return &Postgres{
		instanceID:  instanceID,
		exec:        exec,
		opts:        opts,
		encoder:     encoder,
		insertQuery: fmt.Sprintf(insertQueryFmt, table),
		selectQuery: fmt.Sprintf(selectQueryFmt, table, int(opts.InstanceTimeout/time.Second), table, opts.BatchSize),
		deleteQuery: fmt.Sprintf(deleteQueryFmt, table),
	}
}

func (s *Postgres) Schedule(ctx context.Context, dueDate time.Time, topic string, envelopes ...*pubsub.Envelope) error {
	if len(envelopes) == 0 {
		return nil
	}

	q, args, err := s.buildInsertQuery(dueDate, topic, envelopes...)
	if err != nil {
		return err
	}

	_, err = s.exec.ExecContext(ctx, q, args...)
	if err != nil {
		return fmt.Errorf("cannot insert scheduled messages: %w", err)
	}

	return err
}

func (s *Postgres) ConsumeDue(ctx context.Context) (<-chan pubsub.DueMessage, error) {
	feed := make(chan pubsub.DueMessage)

	go func() {
		t := time.NewTimer(s.opts.MinWaitTime)
		t.Reset(0)
		defer t.Stop()
		defer close(feed)

		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				t.Reset(s.opts.MinWaitTime)

				messages, err := s.listNext(ctx)
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					return
				}
				if err != nil {
					feed <- pubsub.DueMessage{Err: err}
					return
				}

				// blocking here, so we force the scheduler to finish
				// the current batch before stopping after a context cancellation.
				for _, dm := range messages {
					feed <- dm
				}
			}
		}
	}()

	return feed, nil
}

func (s *Postgres) Published(ctx context.Context, message pubsub.DueMessage) error {
	_, err := s.exec.ExecContext(ctx, s.deleteQuery, s.instanceID, message.Envelope.ID)
	if err != nil {
		return fmt.Errorf("cannot delete message: %w", err)
	}

	return nil
}

func (s *Postgres) listNext(ctx context.Context) (messages []pubsub.DueMessage, err error) {
	rows, err := s.exec.QueryContext(ctx, s.selectQuery, s.instanceID)
	if err != nil {
		return nil, fmt.Errorf("cannot select due messages: %w", err)
	}
	for rows.Next() {
		var (
			id    int
			topic string
			raw   []byte
		)
		if err := rows.Scan(&id, &topic, &raw); err != nil {
			return nil, fmt.Errorf("cannot scan due message row: %w", err)
		}
		envelope, err := s.encoder.Decode(raw)
		if err != nil {
			return nil, fmt.Errorf("cannot decode raw due message: %w", err)
		}

		messages = append(messages, pubsub.DueMessage{
			Topic:    topic,
			Envelope: envelope,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("cannot iterate due messages: %w", err)
	}

	return messages, nil
}

func (s *Postgres) buildInsertQuery(dueDate time.Time, topic string, envelopes ...*pubsub.Envelope) (query string, args []interface{}, err error) {
	var q strings.Builder

	n := len(envelopes)

	q.Grow(len(s.insertQuery) + bytesPerInsertRow*n)
	q.WriteString(s.insertQuery)

	args = make([]interface{}, n*4)

	for i, env := range envelopes {
		if i > 0 {
			q.WriteString(",")
		}

		raw, err := s.encoder.Encode(env)
		if err != nil {
			return "", nil, fmt.Errorf("cannot encode message %q: %w", env.ID, err)
		}

		q.WriteByte('(')
		for j, argv := range []interface{}{env.ID, dueDate, topic, raw} {
			if j > 0 {
				q.WriteString(",")
			}
			argc := i*4 + j
			args[argc] = argv
			q.WriteString("$" + strconv.Itoa(argc+1))
		}
		q.WriteByte(')')
	}

	return q.String(), args, nil
}

const (
	insertQueryFmt = `INSERT INTO %s (message_id, due_date, topic, message) VALUES `
	deleteQueryFmt = `
DELETE FROM %s 
WHERE
	instance_id = $1
	AND message_id = $2`

	selectQueryFmt = `
WITH updated AS (
	UPDATE %s
	SET
		instance_id = $1,
		instance_timeout = NOW() + interval 'seconds %d'
	WHERE message_id IN (
		SELECT message_id
			FROM %s
		WHERE
				due_date <= NOW()
			AND (
					instance_id IS NULL
				OR (
					instance_id IS NOT NULL
					AND instance_timeout < NOW()
				)
			)
		ORDER BY due_date ASC, id ASC
		LIMIT %d
		FOR UPDATE SKIP LOCKED
	) RETURNING id, due_date, topic, message
)
SELECT
	id,
	topic,
	message
FROM updated
ORDER BY due_date ASC, id ASC;
`

	bytesPerInsertRow = 13
)
