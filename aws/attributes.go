package aws

import (
	"strings"

	"github.com/aws/aws-sdk-go/service/sns"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/hmoragrega/pubsub"
)

const (
	idAttributeKey        = "id"
	versionAttributeKey   = "version"
	keyAttributeKey       = "key"
	nameAttributeKey      = "name"
	customAttributePrefix = "x-"
)

func encodeAttributes(env *pubsub.Envelope) map[string]*sns.MessageAttributeValue {
	attributes := map[string]*sns.MessageAttributeValue{
		idAttributeKey: {
			DataType:    stringDataType,
			StringValue: &env.ID,
		},
		versionAttributeKey: {
			DataType:    stringDataType,
			StringValue: &env.Version,
		},
		nameAttributeKey: {
			DataType:    stringDataType,
			StringValue: &env.Name,
		},
	}
	if env.Key != "" {
		attributes[keyAttributeKey] = &sns.MessageAttributeValue{
			DataType:    stringDataType,
			StringValue: &env.Key,
		}
	}
	for k, v := range env.Attributes {
		v := v
		k := customAttributePrefix + k
		attributes[k] = &sns.MessageAttributeValue{
			DataType:    stringDataType,
			StringValue: &v,
		}
	}
	return attributes
}

func decodeAttributes(attributes map[string]*sqs.MessageAttributeValue) map[string]string {
	custom := make(map[string]string)
	for k, v := range attributes {
		if strings.Index(k, customAttributePrefix) != 0 {
			continue
		}
		k := k[len(customAttributePrefix):]
		var value string
		if v.StringValue != nil {
			value = *v.StringValue
		}
		custom[k] = value
	}
	return custom
}
