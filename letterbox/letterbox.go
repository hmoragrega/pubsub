package letterbox

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/hmoragrega/pubsub"
)

const (
	responseTopicAttribute = "letterbox-response-topic"
	requestIDAttribute     = "letterbox-request-id"
	requestedAtAttribute   = "letterbox-requested-at"
)

var (
	ErrRequestAlreadySent = errors.New("request already sent")
	ErrSendingRequest     = errors.New("cannot send request")
)

type Response struct {
	RequestID   string
	Request     *pubsub.Message
	Response    *pubsub.Message
	RequestedAt time.Time

	// "miss" indicates that the request was not found and
	// couldn't be answered.
	//
	// Watch out when true as the request will be nil.
	Miss bool
}

type Letterbox struct {
	// OnResponse is an optional callback that will be triggered
	// when a response is received in this letterbox.
	OnResponse func(response *Response)

	Publisher *pubsub.Publisher

	// response topic for this instance.
	Topic string

	requests map[string]requestAddress
	mx       sync.RWMutex
}

// Request sends the request and waits until:
// - the response is available.
// - the given context is done.
func (x *Letterbox) Request(ctx context.Context, topic string, request pubsub.Message) (*pubsub.Message, error) {
	requestID := request.ID
	if len(requestID) == 0 {
		requestID = pubsub.NewID()
	}

	// inject the attributes so we know where to answer.
	request.SetAttribute(responseTopicAttribute, x.Topic)
	request.SetAttribute(requestIDAttribute, requestID)
	request.SetAttribute(requestedAtAttribute, time.Now().Format(time.RFC3339Nano))

	c, err := x.waitFor(requestID, &request)
	if err != nil {
		return nil, err
	}

	// make sure that whatever the result,
	// we remove the pending request.
	defer x.delete(requestID)

	if err := x.Publisher.Publish(ctx, topic, request); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrSendingRequest, err)
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case res := <-c:
		return res.Response, nil
	}
}

// Handler wrap the server handler to accept the responses
func (x *Letterbox) Handler(handler func(context.Context, *pubsub.Message) (*pubsub.Message, error)) pubsub.HandlerFunc {
	return func(ctx context.Context, request *pubsub.Message) error {
		response, err := handler(ctx, request)
		if err != nil {
			return err
		}
		return x.Response(ctx, request, response)
	}
}

// Response sends a response to a request.
func (x *Letterbox) Response(ctx context.Context, request, response *pubsub.Message) error {
	topic, ok := request.Attributes[responseTopicAttribute]
	if !ok {
		return fmt.Errorf("missing response topic in request")
	}
	requestID, ok := request.Attributes[requestIDAttribute]
	if !ok {
		return fmt.Errorf("missing request ID")
	}
	requestedAt, ok := request.Attributes[requestedAtAttribute]
	if !ok {
		return fmt.Errorf("missing request time")
	}

	response.SetAttribute(requestIDAttribute, requestID)
	response.SetAttribute(requestedAtAttribute, requestedAt)

	if err := x.Publisher.Publish(ctx, topic, *response); err != nil {
		return fmt.Errorf("cannot send response: %v", err)
	}
	return nil
}

// HandleMessage is the message handler for the responses
func (x *Letterbox) HandleMessage(_ context.Context, response *pubsub.Message) error {
	requestID, ok := response.Attributes[requestIDAttribute]
	if !ok {
		return fmt.Errorf("missing request ID")
	}

	t := response.Attributes[requestedAtAttribute]
	if !ok {
		return fmt.Errorf("missing request ID")
	}

	requestedAt, err := time.Parse(time.RFC3339Nano, t)
	if err != nil {
		return fmt.Errorf("cannot parse requested at date")
	}

	r := &Response{
		RequestID:   requestID,
		Response:    response,
		RequestedAt: requestedAt,
	}

	address, ok := x.pop(requestID)
	if !ok {
		r.Miss = true
	} else {
		r.Request = address.request
		defer func() {
			address.c <- r
			close(address.c)
		}()
	}
	if f := x.OnResponse; f != nil {
		f(r)
	}
	return nil
}

func (x *Letterbox) pop(id string) (requestAddress, bool) {
	x.mx.Lock()
	defer x.mx.Unlock()
	if x.requests == nil {
		x.requests = make(map[string]requestAddress)
	}

	c, ok := x.requests[id]
	if ok {
		delete(x.requests, id)
	}

	return c, ok
}

type requestAddress struct {
	c       chan *Response
	request *pubsub.Message
}

// waitFor creates a channel to receive the response
// for a request. It fails if the request ID is not unique.
func (x *Letterbox) waitFor(id string, request *pubsub.Message) (<-chan *Response, error) {
	x.mx.Lock()
	defer x.mx.Unlock()
	if x.requests == nil {
		x.requests = make(map[string]requestAddress)
	}

	if _, ok := x.requests[id]; ok {
		return nil, ErrRequestAlreadySent
	}

	c := make(chan *Response, 1)

	x.requests[id] = requestAddress{
		request: request,
		c:       c,
	}

	return c, nil
}

func (x *Letterbox) delete(id string) {
	x.mx.Lock()
	delete(x.requests, id)
	x.mx.Unlock()
}
