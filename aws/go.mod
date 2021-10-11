module github.com/hmoragrega/pubsub/aws

go 1.15

require (
	github.com/aws/aws-sdk-go-v2 v1.3.4
	github.com/aws/aws-sdk-go-v2/config v1.1.1
	github.com/aws/aws-sdk-go-v2/credentials v1.1.1
	github.com/aws/aws-sdk-go-v2/service/sns v1.2.2
	github.com/aws/aws-sdk-go-v2/service/sqs v1.3.1
	github.com/aws/smithy-go v1.3.1
	github.com/hashicorp/go-multierror v1.1.1
	github.com/hmoragrega/pubsub v0.5.0
)

replace github.com/hmoragrega/pubsub => ../
