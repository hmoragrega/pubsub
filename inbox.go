package pubsub

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

var (
	ErrInboxNotInitialized = errors.New("inbox not initialized")
	ErrInboxClosed         = errors.New("inbox is closed")
	ErrNilRequest          = errors.New("request is nil")
	ErrEmptyRequestID      = errors.New("empty request ID")
	ErrRequestAlreadySent  = errors.New("request already sent")
	ErrSendingRequest      = errors.New("cannot send request")
)


type Publisher interface {
	Publish(ctx context.Context, body []byte, attributes Attributes) error
}

type Inbox struct {
	// OnMiss is an optional callback that will be triggered if
	// a response was not expected.
	//
	// This is a normal scenario with timeout, it's responsibility
	// of the caller to decide if it's an error
	OnMiss func(message Message)

	pub      Publisher
	requests map[string]chan Message
	mx       sync.RWMutex
	done     chan struct{}
	started  bool
	stopped  bool
	cancel   func()
}

func NewInbox(pub Publisher) *Inbox {
	i := &Inbox{
		pub:      pub,
		requests: make(map[string]chan Message),
	}
	i.subscribe()
	return i
}

// SendWithTimeout sends the request and waits until:
// - the response is available.
// - the given context is done.
func (x *Inbox) Send(ctx context.Context, request Message) (Message, error) {
	if request == nil {
		return nil, ErrNilRequest
	}
	rID := request.ID()
	if rID == "" {
		return nil, ErrEmptyRequestID
	}
	if err := x.canSend(); err != nil {
		return nil, err
	}
	c, err := x.waitFor(rID)
	if err != nil {
		return nil, err
	}

	// make sure that whatever the result, we clean
	// the pending request.
	defer x.delete(rID)

	if err := x.pub.Publish(ctx, request.Body(), request.Attributes()); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrSendingRequest, err)
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case res := <-c:
		return res, nil
	}
}

func (x *Inbox) Close(ctx context.Context) error {
	x.mx.Lock()
	if x.stopped {
		x.mx.Unlock()
		return nil
	}
	x.stopped = true
	x.cancel()
	x.mx.Unlock()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-x.done:
		return nil
	}
}

// subscribe will start consuming messages from the publisher.
// To stop consuming and free the resources call Close.
func (x *Inbox) subscribe() {
	ctx, cancel := context.WithCancel(context.Background())

	x.mx.Lock()
	x.done = make(chan struct{}, 1)
	x.cancel = cancel
	x.started = true
	x.stopped = false
	x.mx.Unlock()

	go func() {
		defer func() {
			x.done <- struct{}{}
		}()
		for {
			select {
			case <-ctx.Done():
				return
			}
			/*case res := <-x.pub.Subscribe():
				c, ok := x.pop(res.ID())
				if !ok {
					if x.OnMiss != nil {
						x.OnMiss(res)
					}
					continue
				}
				c <- res
			}*/
		}
	}()
}

func (x *Inbox) pop(id string) (chan<- Message, bool) {
	x.mx.Lock()
	defer x.mx.Unlock()
	c, ok := x.requests[id]
	if ok {
		delete(x.requests, id)
	}

	return c, ok
}

func (x *Inbox) canSend() error {
	x.mx.RLock()
	defer x.mx.RUnlock()

	if !x.started {
		return ErrInboxNotInitialized
	}
	if x.stopped {
		return ErrInboxClosed
	}

	return nil
}

// waitFor creates a channel to receive the response
// for a request. It fails if the request ID is not unique.
func (x *Inbox) waitFor(id string) (<-chan Message, error) {
	x.mx.Lock()
	defer x.mx.Unlock()

	if _, ok := x.requests[id]; ok {
		return nil, ErrRequestAlreadySent
	}

	c := make(chan Message, 1)
	x.requests[id] = c

	return c, nil
}

func (x *Inbox) delete(id string) {
	x.mx.Lock()
	delete(x.requests, id)
	x.mx.Unlock()
}
