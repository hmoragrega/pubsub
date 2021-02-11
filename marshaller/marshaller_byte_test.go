package marshaller

import (
	"errors"
	"math/rand"
	"reflect"
	"testing"

	"github.com/hmoragrega/pubsub"
	"github.com/hmoragrega/pubsub/internal/stubs"
)

func TestByteMarshaller_Marshal(t *testing.T) {
	tests := []struct {
		name        string
		data        interface{}
		want        []byte
		wantVersion string
		wantErr     error
	}{
		{
			name:        "string",
			data:        "foo",
			want:        []byte("foo"),
			wantVersion: "byte:s",
		}, {
			name:        "byte slice",
			data:        []byte("foo"),
			want:        []byte("foo"),
			wantVersion: "byte:b",
		}, {
			name:    "error",
			data:    123,
			wantErr: errInvalidDataType,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var m ByteMarshaller

			got, gotVersion, gotErr := m.Marshal(tc.data)

			if !errors.Is(gotErr, tc.wantErr) {
				t.Fatalf("unexpected error; got %+v, want %+v", gotErr, tc.wantErr)
			}
			if gotVersion != tc.wantVersion {
				t.Fatalf("unexpected version; got %+v, want %+v", gotVersion, tc.wantVersion)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("unexpected data; got %+v, want %+v", got, tc.want)
			}
		})
	}
}

func TestByteMarshaller_Unmarshal(t *testing.T) {
	const validKey = "valid"

	buf := make([]byte, 20)
	rand.New(rand.NewSource(1234)).Read(buf)

	dummyAttributes := map[string]string{
		"some": "attribute",
	}

	tests := []struct {
		name        string
		topic       string
		message     *stubs.ReceivedMessageStub
		wantMessage *pubsub.Message
		wantErr     error
	}{
		{
			name: "unknown version",
			message: &stubs.ReceivedMessageStub{
				VersionFunc: func() string {
					return "foo"
				},
			},
			wantErr: errUnknownVersion,
		},
		{
			name: "byte slice",
			message: &stubs.ReceivedMessageStub{
				VersionFunc: func() string {
					return byteSliceVersion
				},
				NameFunc: func() string {
					return validKey
				},
				IDFunc: func() string {
					return "123"
				},
				KeyFunc: func() string {
					return "key"
				},
				AttributesFunc: func() pubsub.Attributes {
					return dummyAttributes
				},
				BodyFunc: func() []byte {
					return buf
				},
			},
			topic: "foo topic",
			wantMessage: &pubsub.Message{
				ID:         "123",
				Name:       validKey,
				Key:        "key",
				Data:       buf,
				Attributes: dummyAttributes,
			},
		},
		{
			name: "string",
			message: &stubs.ReceivedMessageStub{
				VersionFunc: func() string {
					return byteStringVersion
				},
				NameFunc: func() string {
					return validKey
				},
				IDFunc: func() string {
					return "123"
				},
				KeyFunc: func() string {
					return "key"
				},
				AttributesFunc: func() pubsub.Attributes {
					return dummyAttributes
				},
				BodyFunc: func() []byte {
					return buf
				},
			},
			topic: "foo topic",
			wantMessage: &pubsub.Message{
				ID:         "123",
				Name:       validKey,
				Key:        "key",
				Data:       string(buf),
				Attributes: dummyAttributes,
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var m ByteMarshaller

			gotMessage, gotError := m.Unmarshal(tc.topic, tc.message)

			if !errors.Is(gotError, tc.wantErr) {
				t.Fatalf("unpexected error; got %v, want %v", gotError, tc.wantErr)
			}
			if gotError != nil {
				return
			}
			if got, want := gotMessage.ID, tc.wantMessage.ID; got != want {
				t.Errorf("unexpected ID; got %v, want %v", got, want)
			}
			if got, want := gotMessage.Name, tc.wantMessage.Name; got != want {
				t.Errorf("unexpected name; got %v, want %v", got, want)
			}
			if got, want := gotMessage.Key, tc.wantMessage.Key; got != want {
				t.Errorf("unexpected key; got %v, want %v", got, want)
			}
			if got, want := gotMessage.Data, tc.wantMessage.Data; !reflect.DeepEqual(got, want) {
				t.Errorf("unexpected data; got %v, want %v", got, want)
			}
			if got, want := gotMessage.Attributes, tc.wantMessage.Attributes; !reflect.DeepEqual(got, want) {
				t.Errorf("unexpected data; got %v, want %v", got, want)
			}
		})
	}
}
