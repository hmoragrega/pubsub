package aws

import (
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sns"
)

func NewDefaultSNS() (*sns.SNS, error) {
	sess, err := session.NewSession()
	if err != nil {
		return nil, err
	}
	return sns.New(sess), nil
}

func MustSNS(svc *sns.SNS, err error) *sns.SNS {
	if err != nil {
		panic(err)
	}
	return svc
}
