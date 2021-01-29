package aws

import (
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
)

func NewDefaultSQS() (*sqs.SQS, error) {
	sess, err := session.NewSession()
	if err != nil {
		return nil, err
	}
	return sqs.New(sess), nil
}

func MustSQS(svc *sqs.SQS, err error) *sqs.SQS {
	if err != nil {
		panic(err)
	}
	return svc
}
