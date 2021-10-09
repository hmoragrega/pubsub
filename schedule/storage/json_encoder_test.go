package storage

import (
	"github.com/hmoragrega/pubsub"
	"testing"
)

func TestJSONEnvelopeEncoder(t *testing.T) {
	t.Run("it can encode as json a valid envelope", func(t *testing.T) {
		var enc JSONEnvelopeEncoder

		raw, err := enc.Encode(&pubsub.Envelope{ID: "1"})
		if err != nil {
			t.Fatal("unexpected error", err)
		}
		want := `{"ID":"1","Name":"","Key":"","Body":null,"Version":"","Attributes":null}`
		if got := string(raw); got != want {
			t.Fatalf("unexpected encoding; want %s, got %s", want, got)
		}
	})

	t.Run("it can decode a valid json encoded envelope", func(t *testing.T) {
		var enc JSONEnvelopeEncoder

		env, err := enc.Decode([]byte(`{"ID":"1"}`))
		if err != nil {
			t.Fatal("unexpected error", err)
		}
		if env.ID != "1" {
			t.Fatalf("unexpected decoding; got %v", env)
		}
	})

	t.Run("it returns an error on an invalid raw data", func(t *testing.T) {
		var enc JSONEnvelopeEncoder

		_, err := enc.Decode([]byte(`ha!`))
		if err == nil {
			t.Fatal("expected error, got none")
		}
	})
}
