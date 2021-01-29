//+build integration

package aws
/*
import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
)
/*
func TestPublisher_Publish(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	queueURL := createTestQueue(ctx, t, "test-publisher-publish")

	p, err := NewPublisher(
		queueURL,
		WithPublisherSQS(sqsTestInstance),
	)
	if err != nil {
		t.Fatal("cannot create publisher", err)
	}

	var (
		wantReqID = "123"
		wantBody  = "body"
	)

	err = p.Publish(ctx, []byte(wantBody), map[string]string{requestAttrID: wantReqID})
	if err != nil {
		t.Fatal("cannot publish message", err)
	}

	out, err := sqsTestInstance.ReceiveMessageWithContext(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:              &queueURL,
		MessageAttributeNames: []*string{aws.String("All")},
		WaitTimeSeconds:       aws.Int64(1),
	})
	if err != nil {
		t.Fatal("cannot receive message", err)
	}
	if l := len(out.Messages); l != 1 {
		t.Fatalf("expected one message, got %d", l)
	}
	m := out.Messages[0]
	if got := *m.Body; got != wantBody {
		t.Fatalf("message body does not match, got %s, want %s", got, wantBody)
	}
	a, ok := m.MessageAttributes[requestAttrID]
	if !ok {
		t.Fatal("missing request ID attribute value")
	}
	if got := *a.StringValue; got != wantReqID {
		t.Fatalf("request ID attribute does not match, got %s", got)
	}
}
*/
