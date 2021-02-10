package marshaller

import (
	"errors"
	"reflect"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/hmoragrega/pubsub"
	"github.com/hmoragrega/pubsub/internal/proto"
	"github.com/hmoragrega/pubsub/internal/stubs"
)

type testJSONStruct struct {
	Text   string `json:"text"`
	Number int    `json:"number"`
}

func TestJSONMarshaller_Marshal(t *testing.T) {

	tests := []struct {
		name        string
		data        interface{}
		wantPayload []byte
		wantVersion string
		wantErr     error
	}{
		{
			name:        "ok payload string",
			data:        "foo",
			wantVersion: jsonVersion0x01,
			wantPayload: []byte(`"foo"`),
		},
		{
			name: "ok with unicode characters",
			data: &testJSONStruct{
				Text:   "αlpha & Ώmega",
				Number: 12,
			},
			wantVersion: jsonVersion0x01,
			wantPayload: []byte(`{"text":"αlpha \u0026 Ώmega","number":12}`),
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var m JSONMarshaller
			gotPayload, gotVersion, gotError := m.Marshal(tc.data)

			if !errors.Is(gotError, tc.wantErr) {
				t.Fatalf("unpexected error; got %v, want %v", gotError, tc.wantErr)
			}
			if gotVersion != tc.wantVersion {
				t.Fatalf("unpexected version; got %v, want %v", gotVersion, tc.wantVersion)
			}
			if !reflect.DeepEqual(gotPayload, tc.wantPayload) {
				t.Fatalf("unpexected payload; got %s, want %s", gotPayload, tc.wantPayload)
			}
		})
	}
}

func TestJSONMarshaller_Unmarshal(t *testing.T) {
	const validKey = "valid"

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
			name: "unregistered type",
			message: &stubs.ReceivedMessageStub{
				VersionFunc: func() string {
					return jsonVersion0x01
				},
				NameFunc: func() string {
					return "foo name"
				},
			},
			topic:   "foo topic",
			wantErr: errUnregisteredType,
		},
		{
			name: "ok by topic",
			message: &stubs.ReceivedMessageStub{
				VersionFunc: func() string {
					return jsonVersion0x01
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
					return []byte(`{"text":"αlpha \u0026 Ώmega","number":12}`)
				},
			},
			topic: validKey,
			wantMessage: &pubsub.Message{
				ID:   "123",
				Name: "",
				Key:  "key",
				Data: &testJSONStruct{
					Text:   "αlpha & Ώmega",
					Number: 12,
				},
				Attributes: dummyAttributes,
			},
		},
		{
			name: "ok by event name",
			message: &stubs.ReceivedMessageStub{
				VersionFunc: func() string {
					return jsonVersion0x01
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
					return []byte(`{"text":"αlpha \u0026 Ώmega","number":12}`)
				},
			},
			topic: "foo topic",
			wantMessage: &pubsub.Message{
				ID:   "123",
				Name: validKey,
				Key:  "key",
				Data: &testJSONStruct{
					Text:   "αlpha & Ώmega",
					Number: 12,
				},
				Attributes: dummyAttributes,
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var m JSONMarshaller
			if err := m.Register(validKey, &testJSONStruct{}); err != nil {
				t.Fatalf("unpexected error registering a valid type; got %v", err)
			}

			gotMessage, gotError := m.Unmarshal(tc.topic, tc.message)

			if !errors.Is(gotError, tc.wantErr) {
				t.Fatalf("unpexected error; got %v, want %v", gotError, tc.wantErr)
			}
			if diff := cmp.Diff(gotMessage, tc.wantMessage, cmpopts.IgnoreUnexported(proto.Test{})); diff != "" {
				t.Errorf("message mismatch (-got +want):\n%s", diff)
			}
		})
	}
}

func TestJSONMarshaller_UnmarshalErrors(t *testing.T) {
	t.Run("register invalid type", func(t *testing.T) {
		var m JSONMarshaller

		got := m.Register("not-valid", nil)
		if !errors.Is(got, errInvalidDataType) {
			t.Fatalf("unpexected error; got %v, want %v", got, "foo")
		}
	})

	t.Run("unmarshal error", func(t *testing.T) {
		var m JSONMarshaller

		if err := m.Register("valid", &proto.Test{}); err != nil {
			t.Fatalf("unpexected error registering type; got %v", err)
		}

		_, got := m.Unmarshal("foo", &stubs.ReceivedMessageStub{
			VersionFunc: func() string { return jsonVersion0x01 },
			NameFunc:    func() string { return "valid" },
			BodyFunc:    func() []byte { return []byte("not valid json for type") },
		})

		if !errors.Is(got, errUnmarshalling) {
			t.Fatalf("unpexected error; got %v, want %v", got, "foo")
		}
	})

	t.Run("instantiating error", func(t *testing.T) {
		var m JSONMarshaller

		m.registry.types = make(map[string]reflect.Type)
		m.registry.types["not-valid"] = nil

		_, got := m.Unmarshal("foo", &stubs.ReceivedMessageStub{
			VersionFunc: func() string { return jsonVersion0x01 },
			NameFunc:    func() string { return "not-valid" },
			BodyFunc:    func() []byte { return []byte("foo") },
		})

		if !errors.Is(got, errInstantiatingType) {
			t.Fatalf("unpexected error; got %v, want %v", got, "foo")
		}
	})
}
