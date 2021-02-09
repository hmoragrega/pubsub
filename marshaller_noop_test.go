package pubsub

import (
	"errors"
	"reflect"
	"testing"
)

func TestNoOpMarshaller_Marshal(t *testing.T) {
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
			wantVersion: "noop-s",
		}, {
			name:        "byte slice",
			data:        []byte("foo"),
			want:        []byte("foo"),
			wantVersion: "noop-b",
		}, {
			name:    "error",
			data:    123,
			wantErr: errInvalidDataType,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var m NoOpMarshaller

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
