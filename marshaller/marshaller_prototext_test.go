package marshaller

import (
	"errors"
	"math/rand"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/hmoragrega/pubsub"
	"github.com/hmoragrega/pubsub/internal/proto"
	"github.com/hmoragrega/pubsub/internal/stubs"
	"google.golang.org/protobuf/encoding/prototext"
)

func TestProtoTextMarshaller_Marshal(t *testing.T) {
	buf := make([]byte, 10)
	rand.New(rand.NewSource(1234)).Read(buf)

	tests := []struct {
		name        string
		marshaller  *ProtoTextMarshaller
		data        interface{}
		wantPayload []byte
		wantVersion string
		wantErr     error
	}{
		{
			name:    "marshal non proto",
			data:    "foo",
			wantErr: errNonProtoData,
		},
		{
			name:       "default options proto marshal",
			marshaller: &ProtoTextMarshaller{},
			data: &proto.Test{
				Name:    "αlpha",
				Number:  12,
				Payload: buf,
			},
			wantVersion: protoText0x01,
			wantPayload: []byte(`name:"αlpha" number:12 payload:"\xc0\x0e]g\xc2uS\x89\xad\xed"`),
		},
		{
			name: "ascii only proto marshal",
			marshaller: &ProtoTextMarshaller{MarshalOptions: prototext.MarshalOptions{
				EmitASCII: true,
			}},
			data: &proto.Test{
				Name:    "αlpha",
				Number:  12,
				Payload: buf,
			},
			wantVersion: protoText0x01,
			wantPayload: []byte(`name:"\u03b1lpha" number:12 payload:"\xc0\x0e]g\xc2uS\x89\xad\xed"`),
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			gotPayload, gotVersion, gotError := tc.marshaller.Marshal(tc.data)

			if !errors.Is(gotError, tc.wantErr) {
				t.Fatalf("unpexected error; got %v, want %v", gotError, tc.wantErr)
			}
			if gotError != nil {
				return
			}
			if gotVersion != tc.wantVersion {
				t.Fatalf("unpexected version; got %v, want %v", gotVersion, tc.wantVersion)
			}

			// proto encoding output is not stable, we need to compare
			// against the expected unmarshalled message.
			// @see https://github.com/golang/protobuf/issues/1121
			var want proto.Test
			err := prototext.Unmarshal(gotPayload, &want)
			if err != nil {
				t.Fatalf("unpexected error unmarshalling result; got %v", err)
			}
			if diff := cmp.Diff(tc.data, &want, cmpopts.IgnoreUnexported(proto.Test{})); diff != "" {
				t.Errorf("payload mismatch (-got +want):\n%s", diff)
			}
		})
	}
}

func TestProtoTextMarshaller_Unmarshal(t *testing.T) {
	const validKey = "valid"

	dummyAttributes := map[string]string{
		"some": "attribute",
	}

	tests := []struct {
		name         string
		unmarshaller *ProtoTextMarshaller
		topic        string
		message      *stubs.ReceivedMessageStub
		wantMessage  *pubsub.Message
		wantErr      error
	}{
		{
			name:         "unknown version",
			unmarshaller: &ProtoTextMarshaller{},
			message: &stubs.ReceivedMessageStub{
				VersionFunc: func() string {
					return "foo"
				},
			},
			wantErr: errUnknownVersion,
		},
		{
			name:         "unregistered type",
			unmarshaller: &ProtoTextMarshaller{},
			message: &stubs.ReceivedMessageStub{
				VersionFunc: func() string {
					return protoText0x01
				},
				NameFunc: func() string {
					return "foo name"
				},
			},
			topic:   "foo topic",
			wantErr: errUnregisteredType,
		},
		{
			name:         "ok by topic with ascii only",
			unmarshaller: &ProtoTextMarshaller{},
			message: &stubs.ReceivedMessageStub{
				VersionFunc: func() string {
					return protoText0x01
				},
				NameFunc: func() string {
					return ""
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
					return []byte(`name:"\u03b1lpha" number:12`)
				},
			},
			topic: validKey,
			wantMessage: &pubsub.Message{
				ID:   "123",
				Name: "",
				Key:  "key",
				Data: &proto.Test{
					Name:   "αlpha",
					Number: 12,
				},
				Attributes: dummyAttributes,
			},
		},
		{
			name:         "ok by event name with unicode characters",
			unmarshaller: &ProtoTextMarshaller{},
			message: &stubs.ReceivedMessageStub{
				VersionFunc: func() string {
					return protoText0x01
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
					return []byte(`name:"αlpha" number:12`)
				},
			},
			topic: "foo topic",
			wantMessage: &pubsub.Message{
				ID:   "123",
				Name: validKey,
				Key:  "key",
				Data: &proto.Test{
					Name:   "αlpha",
					Number: 12,
				},
				Attributes: dummyAttributes,
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.unmarshaller.Register(validKey, &proto.Test{}); err != nil {
				t.Fatalf("unpexected error registering a valid type; got %v", err)
			}

			gotMessage, gotError := tc.unmarshaller.Unmarshal(tc.topic, tc.message)

			if !errors.Is(gotError, tc.wantErr) {
				t.Fatalf("unpexected error; got %v, want %v", gotError, tc.wantErr)
			}
			if diff := cmp.Diff(gotMessage, tc.wantMessage, cmpopts.IgnoreUnexported(proto.Test{})); diff != "" {
				t.Errorf("message mismatch (-got +want):\n%s", diff)
			}
		})
	}
}

func TestProtoTextMarshaller_UnmarshalErrors(t *testing.T) {
	t.Run("invalid type", func(t *testing.T) {
		var m ProtoTextMarshaller

		if err := m.registry.register("not-valid", &ProtoTextMarshaller{}); err != nil {
			t.Fatalf("unpexected error registering type; got %v", err)
		}

		_, got := m.Unmarshal("foo", &stubs.ReceivedMessageStub{
			VersionFunc: func() string { return protoText0x01 },
			NameFunc:    func() string { return "not-valid" },
			BodyFunc:    func() []byte { return nil },
		})

		if !errors.Is(got, errInstantiatingType) {
			t.Fatalf("unpexected error; got %v, want %v", got, "foo")
		}
	})

	t.Run("unmarshal error", func(t *testing.T) {
		var m ProtoTextMarshaller

		if err := m.Register("valid", &proto.Test{}); err != nil {
			t.Fatalf("unpexected error registering type; got %v", err)
		}

		_, got := m.Unmarshal("foo", &stubs.ReceivedMessageStub{
			VersionFunc: func() string { return protoText0x01 },
			NameFunc:    func() string { return "valid" },
			BodyFunc:    func() []byte { return []byte("not valid proto text") },
		})

		if !errors.Is(got, errUnmarshalling) {
			t.Fatalf("unpexected error; got %v, want %v", got, "foo")
		}
	})

	t.Run("already registered", func(t *testing.T) {
		var m ProtoTextMarshaller

		if err := m.Register("valid", &proto.Test{}); err != nil {
			t.Fatalf("unpexected error registering type; got %v", err)
		}

		got := m.Register("valid", &proto.Test{})
		if !errors.Is(got, errAlreadyRegistered) {
			t.Fatalf("unpexected error registering type; got %v; want %v", got, errAlreadyRegistered)
		}
	})
}
