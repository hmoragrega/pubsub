package pubsub

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"sync"
	"time"
)

const (
	responseTopicAttribute = "inbox-response-topic"
	requestIDAttribute     = "inbox-request-id"
	requestedAtAttribute   = "inbox-requested-at"
)

var (
	ErrRequestAlreadySent = errors.New("request already sent")
	ErrSendingRequest     = errors.New("cannot send request")
)

type Response struct {
	RequestID   string
	Request     *Message
	Response    *Message
	RequestedAt time.Time

	// "miss" indicates that the request was not found and
	// couldn't be answered.
	//
	// Watch out when true as the request will be nil.
	Miss bool
}

type Inbox struct {
	// OnResponse is an optional callback that will be triggered
	// when a response is received in this inbox.
	OnResponse func(response *Response)

	Publisher *Publisher

	// response topic for this instance.
	Topic string

	requests map[string]requestAddress
	mx       sync.RWMutex
}

// SendWithTimeout sends the request and waits until:
// - the response is available.
// - the given context is done.
func (x *Inbox) Request(ctx context.Context, topic string, request Message) (*Message, error) {
	var id []byte
	if len(request.ID) > 0 {
		id = request.ID
	} else {
		id = NewID()
	}
	requestID := base64.StdEncoding.EncodeToString(id)

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

// Response sends a response to a request.
func (x *Inbox) Response(ctx context.Context, request, response *Message) error {
	topic := request.GetAttribute(responseTopicAttribute)
	if topic == "" {
		return fmt.Errorf("missing response topic in request")
	}
	requestID := request.GetAttribute(requestIDAttribute)
	if requestID == "" {
		return fmt.Errorf("missing request ID")
	}

	requestedAt := request.GetAttribute(requestedAtAttribute)

	response.SetAttribute(requestIDAttribute, requestID)
	response.SetAttribute(requestedAtAttribute, requestedAt)

	if err := x.Publisher.Publish(ctx, topic, *response); err != nil {
		return fmt.Errorf("cannot send response: %v", err)
	}
	return nil
}

// HandleMessage is the message handler for the responses
func (x *Inbox) HandleMessage(_ context.Context, response *Message) error {
	requestID := response.GetAttribute(requestIDAttribute)
	if requestID == "" {
		return fmt.Errorf("missing request ID")
	}

	var requestedAt time.Time
	if t := response.GetAttribute(requestedAtAttribute); t != "" {
		requestedAt, _ = time.Parse(time.RFC3339Nano, t)
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

func (x *Inbox) pop(id string) (requestAddress, bool) {
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
	request *Message
}

// waitFor creates a channel to receive the response
// for a request. It fails if the request ID is not unique.
func (x *Inbox) waitFor(id string, request *Message) (<-chan *Response, error) {
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

func (x *Inbox) delete(id string) {
	x.mx.Lock()
	delete(x.requests, id)
	x.mx.Unlock()
}
