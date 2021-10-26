package aws

import (
	"strings"

	snstypes "github.com/aws/aws-sdk-go-v2/service/sns/types"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/hmoragrega/pubsub"
)

const (
	idAttributeKey        = "id"
	versionAttributeKey   = "version"
	keyAttributeKey       = "key"
	nameAttributeKey      = "name"
	receiveCountAttrKey   = "receive-count"
	customAttributePrefix = "x-"
)

func encodeSQSAttributes(env *pubsub.Envelope) map[string]sqstypes.MessageAttributeValue {
	rawAttributes := encodeAttributes(env)
	sqsAttributes := make(map[string]sqstypes.MessageAttributeValue, len(rawAttributes))
	for k, v := range rawAttributes {
		v := v
		sqsAttributes[k] = sqstypes.MessageAttributeValue{
			DataType:    stringDataType,
			StringValue: &v,
		}
	}

	return sqsAttributes
}

func encodeSNSAttributes(env *pubsub.Envelope) map[string]snstypes.MessageAttributeValue {
	rawAttributes := encodeAttributes(env)
	snsAttributes := make(map[string]snstypes.MessageAttributeValue, len(rawAttributes))
	for k, v := range rawAttributes {
		v := v
		snsAttributes[k] = snstypes.MessageAttributeValue{
			DataType:    stringDataType,
			StringValue: &v,
		}
	}

	return snsAttributes
}

func encodeAttributes(env *pubsub.Envelope) map[string]string {
	attributes := map[string]string{
		idAttributeKey:      env.ID,
		versionAttributeKey: env.Version,
	}
	if env.Name != "" {
		attributes[nameAttributeKey] = env.Name
	}
	if env.Key != "" {
		attributes[keyAttributeKey] = env.Key
	}
	if v, ok := env.Attributes[receiveCountAttrKey]; ok {
		attributes[receiveCountAttrKey] = v
	}
	for k, v := range env.Attributes {
		attributes[customAttributePrefix+k] = v
	}

	// drop the receive-count as custom attribute.
	delete(attributes, customAttributePrefix+receiveCountAttrKey)

	return attributes
}

func decodeCustomAttributes(attributes map[string]sqstypes.MessageAttributeValue) map[string]string {
	custom := make(map[string]string)
	for k, v := range attributes {
		if !strings.HasPrefix(k, customAttributePrefix) {
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
