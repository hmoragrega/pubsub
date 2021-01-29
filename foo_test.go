package pubsub

import (
	"encoding/json"
	"errors"
	"reflect"
	"testing"
)

type AWSRoles []string

func (r *AWSRoles) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err == nil {
		*r = append(*r, s)
		return nil
	}
	var ss []string
	if err := json.Unmarshal(b, &ss); err == nil {
		*r = ss
		return nil
	}
	return errors.New("cannot unmarshal neither to a string nor a slice of strings")
}

type AWSPolicy struct {
	Statement []struct {
		Principal struct {
			AWSRoles AWSRoles `json:"AWS"`
		} `json:"Principal"`
	} `json:"Statement"`
}

var testsAWSPolicyParsing = []struct {
	name      string
	input     []byte
	wantRoles AWSRoles
}{
	{
		name:      "unique role",
		input:     []byte(`{"Version":"2012-10-17","Statement":[{"Sid":"","Effect":"Allow","Principal":{"AWS":"arn:aws:sts::<account>:assumed-role/custom_role/<role>"},"Action":"sts:AssumeRole","Condition":{"StringEquals":{"sts:ExternalId":"<account>"}}}]}`),
		wantRoles: AWSRoles{"arn:aws:sts::<account>:assumed-role/custom_role/<role>"},
	},
	{
		name:  "multiple roles",
		input: []byte(`{"Version":"2012-10-17","Statement":[{"Sid":"","Effect":"Allow","Principal":{"AWS":["arn:aws:sts::<account>:assumed-role/custom_role/<role_1>","arn:aws:sts::<account>:assumed-role/custom_role/<role_2>"]},"Action":"sts:AssumeRole","Condition":{"StringEquals":{"sts:ExternalId":"<account>"}}}]}`),
		wantRoles: AWSRoles{
			"arn:aws:sts::<account>:assumed-role/custom_role/<role_1>",
			"arn:aws:sts::<account>:assumed-role/custom_role/<role_2>",
		},
	},
}

func TestParseAWSPolicy(t *testing.T) {
	for _, tc := range testsAWSPolicyParsing {
		t.Run(tc.name, func(t *testing.T) {
			var p AWSPolicy
			err := json.Unmarshal(tc.input, &p)
			if err != nil {
				t.Fatal("unexpected error parsing AWSRoles policy", err)
			}
			if l := len(p.Statement); l != 1 {
				t.Fatalf("unexpected Statement length. want 1, got %d", l)
			}
			if got := p.Statement[0].Principal.AWSRoles; !reflect.DeepEqual(got, tc.wantRoles) {
				t.Fatalf("roles are not the same, got %v, want %v", got, tc.wantRoles)
			}
		})
	}
}
