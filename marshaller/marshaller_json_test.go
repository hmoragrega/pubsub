package marshaller

import (
	"errors"
	"reflect"
	"testing"

	"github.com/hmoragrega/pubsub"
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
			wantErr: pubsub.ErrUnsupportedVersion,
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
			wantErr: ErrUnregisteredType,
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

func TestJSONMarshaller_UnmarshalErrors(t *testing.T) {
	t.Run("register invalid type", func(t *testing.T) {
		var m JSONMarshaller

		got := m.Register("not-valid", nil)
		if !errors.Is(got, ErrInvalidDataType) {
			t.Fatalf("unpexected error; got %v, want %v", got, "foo")
		}
	})

	t.Run("unmarshal error", func(t *testing.T) {
		var m JSONMarshaller

		if err := m.Register("valid", &testStruct{}); err != nil {
			t.Fatalf("unpexected error registering type; got %v", err)
		}

		_, got := m.Unmarshal("foo", &stubs.ReceivedMessageStub{
			VersionFunc: func() string { return jsonVersion0x01 },
			NameFunc:    func() string { return "valid" },
			BodyFunc:    func() []byte { return []byte("not valid json for type") },
		})

		if !errors.Is(got, ErrUnmarshalling) {
			t.Fatalf("unpexected error; got %v, want %v", got, "foo")
		}
	})
}

type testStruct struct {
	ID   int
	Name string
}
