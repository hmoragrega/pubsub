//+build integration

package letterbox

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	awssdk "github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sns"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/hmoragrega/pubsub"
	"github.com/hmoragrega/pubsub/aws"
	"github.com/hmoragrega/pubsub/internal/env"
	"github.com/hmoragrega/pubsub/marshaller"
)

type sumRequest struct {
	A int
	B int
}

type sumResponse struct {
	X int
}

type subtractRequest struct {
	A int
	B int
}

type subtractResponse struct {
	X int
}

var sqsTest *sqs.SQS
var snsTest *sns.SNS

func TestMain(m *testing.M) {
	cfg := awssdk.Config{
		Region: awssdk.String(env.GetEnvOrDefault("AWS_REGION", "eu-west-3")),
	}
	if os.Getenv("AWS") != "true" {
		cfg.Credentials = credentials.NewStaticCredentials("id", "secret", "token")
		cfg.Endpoint = awssdk.String(env.GetEnvOrDefault("AWS_ENDPOINT", "localhost:4100"))
		cfg.DisableSSL = awssdk.Bool(true)
	}
	sess, err := session.NewSessionWithOptions(session.Options{Config: cfg})
	if err != nil {
		panic(err)
	}
	sqsTest = sqs.New(sess)
	snsTest = sns.New(sess)
	m.Run()
}

func TestLetterbox(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	var (
		instanceTopic       = fmt.Sprintf("letterbox-instance-%d", rand.Int31())
		instanceQueue       = fmt.Sprintf("%s-queue", instanceTopic)
		mathSvcTopic        = fmt.Sprintf("letterbox-math-svc-%d", rand.Int31())
		mathSvcQueue        = fmt.Sprintf("%s-queue", mathSvcTopic)
		instanceTopicARN    = aws.MustGetResource(aws.CreateTopic(ctx, snsTest, instanceTopic))
		mathServiceTopicARN = aws.MustGetResource(aws.CreateTopic(ctx, snsTest, mathSvcTopic))
		instanceQueueURL    = aws.MustGetResource(aws.CreateQueue(ctx, sqsTest, instanceQueue))
		mathSvcQueueURL     = aws.MustGetResource(aws.CreateQueue(ctx, sqsTest, mathSvcQueue))
		instanceQueueARN    = aws.MustGetResource(aws.GetQueueARN(ctx, sqsTest, instanceQueueURL))
		mathSvcQueueARN     = aws.MustGetResource(aws.GetQueueARN(ctx, sqsTest, mathSvcQueueURL))
		instanceSub         = aws.MustGetResource(aws.Subscribe(ctx, snsTest, instanceTopicARN, instanceQueueARN))
		mathSvcSub          = aws.MustGetResource(aws.Subscribe(ctx, snsTest, mathServiceTopicARN, mathSvcQueueARN))
		jsonMarshaller      marshaller.JSONMarshaller
	)
	aws.Must(aws.CreateForwardingPolicy(ctx, sqsTest, instanceQueueURL, instanceQueueARN, instanceTopicARN))
	aws.Must(aws.CreateForwardingPolicy(ctx, sqsTest, mathSvcQueueURL, mathSvcQueueARN, mathServiceTopicARN))

	t.Cleanup(func() {
		ctx := context.Background()
		aws.Must(aws.Unsubscribe(ctx, snsTest, mathSvcSub))
		aws.Must(aws.Unsubscribe(ctx, snsTest, instanceSub))
		aws.Must(aws.DeleteQueue(ctx, sqsTest, mathSvcQueueURL))
		aws.Must(aws.DeleteQueue(ctx, sqsTest, instanceQueueURL))
		aws.Must(aws.DeleteTopic(ctx, snsTest, mathServiceTopicARN))
		aws.Must(aws.DeleteTopic(ctx, snsTest, instanceTopicARN))
	})

	publisher := &pubsub.Publisher{
		Publisher: &aws.Publisher{
			SNS: snsTest,
			TopicARNs: map[string]string{
				mathSvcTopic: mathServiceTopicARN,
			}},
		Marshaller: &jsonMarshaller,
	}

	var (
		sumRequestEventName       = "sum-request"
		sumResponseEventName      = "sum-response"
		subtractRequestEventName  = "subtract-request"
		subtractResponseEventName = "subtract-response"
	)

	jsonMarshaller.Register(sumRequestEventName, &sumRequest{})
	jsonMarshaller.Register(sumResponseEventName, &sumResponse{})
	jsonMarshaller.Register(subtractRequestEventName, &subtractRequest{})
	jsonMarshaller.Register(subtractResponseEventName, &subtractResponse{})

	letterbox := &Letterbox{
		Publisher: publisher,
		Topic:     instanceTopicARN,
	}

	router := pubsub.Router{
		Unmarshaller: &jsonMarshaller,
	}

	err := router.Register(
		instanceTopic,
		&aws.Subscriber{SQS: sqsTest, QueueURL: instanceQueueURL},
		pubsub.Dispatcher(map[string]pubsub.Handler{
			sumResponseEventName:      letterbox,
			subtractResponseEventName: letterbox,
		}),
	)
	if err != nil {
		t.Fatal("cannot register instance subscriber", err)
	}

	err = router.Register(
		mathSvcTopic,
		&aws.Subscriber{SQS: sqsTest, QueueURL: mathSvcQueueURL},
		pubsub.Dispatcher(map[string]pubsub.Handler{
			sumRequestEventName: letterbox.Handler(
				func(ctx context.Context, request *pubsub.Message) (*pubsub.Message, error) {
					req := request.Data.(*sumRequest)
					return &pubsub.Message{
						Name: sumResponseEventName,
						Data: &sumResponse{X: req.A + req.B},
					}, nil
				}),
			subtractRequestEventName: letterbox.Handler(
				func(ctx context.Context, request *pubsub.Message) (*pubsub.Message, error) {
					req := request.Data.(*subtractRequest)
					return &pubsub.Message{
						Name: subtractResponseEventName,
						Data: &subtractResponse{X: req.A - req.B},
					}, nil
				}),
		}),
	)
	if err != nil {
		t.Fatal("cannot register math service subscriber", err)
	}

	routerStopped := make(chan error)
	go func() {
		routerStopped <- router.Run(ctx)
	}()

	// do a sum
	res, err := letterbox.Request(ctx, mathSvcTopic, pubsub.Message{
		Name: sumRequestEventName,
		Data: &sumRequest{A: 9, B: 5},
	})
	if err != nil {
		t.Fatal("error requesting a sum", err)
	}
	if x := res.Data.(*sumResponse).X; x != 9+5 {
		t.Fatalf("math service is drunk, 9 + 5 != %d", x)
	}

	// do a subtraction
	res, err = letterbox.Request(ctx, mathSvcTopic, pubsub.Message{
		Name: subtractRequestEventName,
		Data: &subtractRequest{A: 2, B: 8},
	})
	if err != nil {
		t.Fatal("error requesting a subtraction", err)
	}
	if x := res.Data.(*subtractResponse).X; x != 2-8 {
		t.Fatalf("math service is drunk, 2 - 8 != %d", x)
	}

	cancel()

	select {
	case <-time.NewTimer(time.Second).C:
		t.Fatal("timeout waiting for a clean stop!")
	case err := <-routerStopped:
		if err != nil {
			t.Fatal("router stopped with an error!", err)
		}
	}
}
