//+build integration

package postgres

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v4/stdlib"

	"github.com/hmoragrega/pubsub"
)

func TestPostgresStorage(t *testing.T) {
	ctx := context.Background()
	instanceID := "123"

	t.Run("can schedule messages and retrieve them ordered at the right time", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		t.Cleanup(cancel)

		svc := NewPostgres(instanceID, "scheduled", db(t))

		delay := 2 * time.Second
		topic := "foo"
		msg := &pubsub.Envelope{
			ID:         "1",
			Name:       "name",
			Key:        "key",
			Body:       []byte("body"),
			Version:    "version",
			Attributes: map[string]string{"foo": "bar"},
		}
		msg2 := &pubsub.Envelope{ID: "2"}
		msg3 := &pubsub.Envelope{ID: "3"}

		scheduleTime := time.Now()
		if err := svc.Schedule(ctx, time.Now().Add(delay), topic, msg, msg2, msg3); err != nil {
			t.Fatal(err)
		}

		c, err := svc.ConsumeDue(ctx)
		if err != nil {
			t.Fatal(err)
		}

		select {
		case <-ctx.Done():
			t.Fatal("timeout!")
		case dm := <-c:
			if dm.Err != nil {
				t.Fatal(dm.Err)
			}
			elapsed := time.Now().Sub(scheduleTime).Round(time.Second)
			if elapsed != delay {
				t.Fatalf("time between schedule and recover is not correct; got %v", elapsed)
			}
			if got := dm.Topic; got != topic {
				t.Fatalf("unexpected topic; expected %s, got %s", topic, got)
			}
			if got := dm.Envelope.ID; got != msg.ID {
				t.Fatalf("unexpected message ID; expected %s, got %s", msg.ID, got)
			}
			if got := dm.Envelope.Name; got != msg.Name {
				t.Fatalf("unexpected name; expected %s, got %s", msg.Name, got)
			}
			if got := dm.Envelope.Key; got != msg.Key {
				t.Fatalf("unexpected key; expected %s, got %s", msg.Name, got)
			}
			if bytes.Compare(dm.Envelope.Body, msg.Body) != 0 {
				t.Fatalf("unexpected body; expected %v, got %v", msg.Body, dm.Envelope.Body)
			}
			if dm.Envelope.Attributes["foo"] != "bar" {
				t.Fatalf("unexpected attributes; expected %v, got %v", msg.Attributes, dm.Envelope.Attributes)
			}
		}
	})

	t.Run("multiple instance can run concurrently and fail over to eventually publish all due messages", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(ctx, time.Minute)
		t.Cleanup(cancel)

		numMessages := 1000
		options := Options{
			BatchSize:       30,
			MinWaitTime:     250 * time.Millisecond,
			InstanceTimeout: 5 * time.Second,
		}

		svc1 := NewPostgresWithOptions("1", "scheduled", db(t), options)
		svc2 := NewPostgresWithOptions("2", "scheduled", db(t), options)
		svc3 := NewPostgresWithOptions("3", "scheduled", db(t), options)

		services := []*Postgres{svc1, svc2, svc3}

		// Schedule N due messages
		messages := make(map[string]pubsub.DueMessage, numMessages)
		for i := 1; i <= numMessages; i++ {
			msgID := strconv.Itoa(i)
			m := pubsub.DueMessage{
				Topic: "topic-" + msgID,
				Envelope: &pubsub.Envelope{
					ID:         msgID,
					Name:       "name" + msgID,
					Key:        "key" + msgID,
					Body:       []byte("body" + msgID),
					Version:    "version" + msgID,
					Attributes: map[string]string{"foo": msgID},
				},
			}
			messages[msgID] = m

			if err := svc1.Schedule(ctx, time.Now(), m.Topic, m.Envelope); err != nil {
				t.Fatal(err)
			}
		}

		type svcMessage struct {
			dm  pubsub.DueMessage
			svc *Postgres
			sID int
		}

		// Start consuming from all 3 services
		feed := make(chan svcMessage)
		feeds := make([]<-chan pubsub.DueMessage, 3)
		for i, svc := range services {
			svcFeed, err := svc.ConsumeDue(ctx)
			if err != nil {
				t.Fatal(err)
			}
			feeds[i] = svcFeed
			// fan in all messages into a single feed.
			go func(i int, svc *Postgres) {
				var count int
				for m := range svcFeed {
					count++
					// stop consuming and block the queue during to simulate a crash on the instance
					// The other working services should pick up its messages after the timeout.
					if i == 0 && count == int(float64(options.BatchSize)*5.5) {
						debugf(t, "[1] stopped service after processing %d messages\n", count)
						return
					}
					if i == 1 && count == int(float64(options.BatchSize)*10.5) {
						debugf(t, "[2] stopped service after processing %d messages\n", count)
						return
					}

					feed <- svcMessage{dm: m, svc: svc, sID: i + 1}
				}
			}(i, svc)
		}

		for remaining := len(messages); remaining > 0; {
			select {
			case <-ctx.Done():
				t.Fatalf("timeout! still %d pending messages to be acked", remaining)
			case sm := <-feed:
				dm := sm.dm
				if dm.Err != nil {
					t.Fatal(dm.Err)
				}

				gotID := dm.Envelope.ID
				expected, ok := messages[gotID]
				if !ok {
					t.Fatalf("unexpected message with ID %q; got %v", gotID, dm)
				}

				requireEqual(t, expected, dm)

				if err := sm.svc.Published(ctx, dm); err != nil {
					t.Fatal(err)
				}

				delete(messages, gotID)
				remaining = len(messages)

				debugf(t, "[%d] deleted message %s\n", sm.sID, gotID)
			}
		}

		// signal the services to stop consuming.
		cancel()

		var pending int
		for i, feed := range []<-chan pubsub.DueMessage{feeds[0], feeds[1]} {
			for dm := range feed {
				debugf(t, "[%d] pending message %s", i+1, dm.Envelope.ID)
				pending++
			}
		}
		// Half batch should have been pending for each service.
		if exp := options.BatchSize / 2 * 2; pending != exp {
			t.Fatalf("unexpected number of pending messages in the services, expected %d, got %d", exp, pending)
		}
	})
}

func requireEqual(t *testing.T, expected, got pubsub.DueMessage) {
	if got.Topic != expected.Topic {
		t.Fatalf("unexpected topic; expected %s, got %s", expected.Topic, got.Topic)
	}
	expEnv, gotEnv := expected.Envelope, got.Envelope
	if gotEnv.ID != expEnv.ID {
		t.Fatalf("unexpected message ID; expected %s, got %s", expEnv.ID, gotEnv.ID)
	}
	if gotEnv.Name != expEnv.Name {
		t.Fatalf("unexpected name; expected %s, got %s", expEnv.Name, gotEnv.Name)
	}
	if gotEnv.Key != expEnv.Key {
		t.Fatalf("unexpected key; expected %s, got %s", expEnv.Key, gotEnv.Key)
	}
	if bytes.Compare(expEnv.Body, gotEnv.Body) != 0 {
		t.Fatalf("unexpected body; expected %v, got %v", string(expEnv.Body), string(gotEnv.Body))
	}
	if !reflect.DeepEqual(expEnv.Attributes, gotEnv.Attributes) {
		t.Fatalf("unexpected attributes; expected %v, got %v", expEnv.Attributes, gotEnv.Attributes)
	}
}

func db(t *testing.T) *sql.DB {
	db, err := sql.Open("pgx", dsn())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = db.Close()
	})
	if err := db.PingContext(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := migrate(db); err != nil {
		t.Fatal(err)
	}

	return db
}

func migrate(db *sql.DB) error {
	_, err := db.Exec(`
CREATE TABLE IF NOT EXISTS scheduled (
    id BIGSERIAL PRIMARY KEY,
    message_id TEXT NOT NULL,
    due_date TIMESTAMP WITH TIME ZONE NOT NULL,
    topic TEXT NOT NULL,
    message BYTEA NOT NULL,
    instance_id TEXT DEFAULT NULL, 
    instance_timeout TIMESTAMP WITH TIME ZONE DEFAULT NULL,

    CONSTRAINT message_id_idx UNIQUE (message_id)
)`)
	if err != nil {
		return fmt.Errorf("cannot create table: %w", err)
	}
	_, err = db.Exec(`TRUNCATE scheduled`)
	if err != nil {
		return fmt.Errorf("cannot truncate table: %w", err)
	}
	return nil
}

func debugf(t *testing.T, format string, args ...interface{}) {
	if testing.Verbose() {
		t.Logf(format, args...)
	}
}

func dsn() string {
	dsn := os.Getenv("POSTGRES_DSN")
	if dsn == "" {
		dsn = "postgres://root:test@127.0.0.1:5432/pubsub?sslmode=disable"
	}
	return dsn
}
