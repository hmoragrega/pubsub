package marshaller

import (
	"errors"
	"math/rand"
	"reflect"
	"testing"

	"github.com/hmoragrega/pubsub"
	"github.com/hmoragrega/pubsub/pubsubtest/stubs"
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
			wantErr: ErrInvalidDataType,
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
	buf := make([]byte, 20)
	rand.New(rand.NewSource(1234)).Read(buf)

	tests := []struct {
		name     string
		topic    string
		message  *stubs.ReceivedMessageStub
		wantData interface{}
		wantErr  error
	}{
		{
			name: "unknown version",
			message: &stubs.ReceivedMessageStub{
				VersionFunc: func() string {
					return "foo"
				},
			},
			wantErr: pubsub.ErrUnsupportedVersion,
		},
		{
			name: "byte slice",
			message: &stubs.ReceivedMessageStub{
				VersionFunc: func() string {
					return byteSliceVersion
				},
				BodyFunc: func() []byte {
					return buf
				},
			},
			topic:    "foo topic",
			wantData: buf,
		},
		{
			name: "string",
			message: &stubs.ReceivedMessageStub{
				VersionFunc: func() string {
					return byteStringVersion
				},
				BodyFunc: func() []byte {
					return buf
				},
			},
			topic:    "foo topic",
			wantData: string(buf),
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var m ByteMarshaller

			gotData, gotError := m.Unmarshal(tc.topic, tc.message)

			if !errors.Is(gotError, tc.wantErr) {
				t.Fatalf("unpexected error; got %v, want %v", gotError, tc.wantErr)
			}
			if gotError != nil {
				return
			}
			if got, want := gotData, tc.wantData; !reflect.DeepEqual(got, want) {
				t.Errorf("unexpected data; got %v, want %v", got, want)
			}
		})
	}
}
