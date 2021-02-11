# Pub/Sub

[![godoc][godoc-badge]][godoc-url]
[![ci][ci-badge]][ci-url]
[![coverage][coverage-badge]][coverage-url]
[![goreport][goreport-badge]][goreport-url]

Package for publishing and consuming messages from pub/sub systems.

Supported:
 * [AWS](aws/README.md) (SNS+SQS)

## TL;DR
Publish a message
```go
publisher := pubsub.NewPublisher(
	awsSNSPublisher,
	&marshaller.ProtoTextMarshaller{},
)

publisher.Publish(ctx, "some-topic", &pubsub.Message{
    Data: &proto.SomeMessage {Foo: "bar"},
})
```

Consume messages
```go
var unmarshaller marshaller.ProtoTextMarshaller
unmarshaller.Register("some-topic", &proto.SomeMessage{})

router := pubsub.Router{
	Unmarshaller: &unmarshaller,
}

router.Register(
    "some-topic",
    awsSQSSusbcriber,
    pubsub.HandlerFunc(func(ctx context.Context, message *pubsub.Message) error {
        msg := request.Data.(*proto.SomeMessage)
        fmt.Println(msg.Foo)
    })
)

router.Run(ctx)
```

## Components
### Message
The key component in the flow is the [message](message.go#L38), this is the struct that 
is going to be published, and the struct that your handlers will receive.     

The message contains this information:    
* **Data:** it accepts any type (accepts `interface{}`), as long as the marshaller
you've chosen supports serializing it to a `[]byte`.     
* **ID:** (optional) a `string` with an ID for the message, if not provided, the publisher
 will add one UUIDv4 automatically.     
* **Name:** (optional) name for the message, it can be used to [dispatch the messages](handler.go#L28) using the name as 
discriminator while sharing the same topic.     
* **Key:** (optional) certain pub/sub system can leverage the key to provide FIFO semantics to
messages with the same key within a single topic.      
* **Attributes:** (optional) a `map[string]string` with even custom metadata.

### Publisher
The publisher sends the message to a given topic using the underlying pub/sub system implementation.    

```go
type Publisher interface {
	Publish(ctx context.Context, topic string, messages ...*Message) error
}
``` 

Steps:
* Serializes the message data into using the marshaller
* Sends the data, and the rest of the message fields
as an `EnvelopeMessage` to the pub/sub system.     

#### Envelope publisher
This is the specific sender for each pub/sub system, it receives an [envelope](publisher.go#L24), that holds
the data for the message (this time as `[]byte`), the envelope has also the version of the marshaller 
used to serialize the data.    

You can check the implementation for Amazon's [SNS service here](aws/publisher.go#L24) as example.     

### Subscriber
The subscriber can subscribe to a single topic in a pub/sub system and consume the topic 
messages.     

```go
// Subscriber consumes messages from a topic.
type Subscriber interface {
	// Subscribe to the topic.
	Subscribe() (<-chan Next, error)

	// Stop stops consuming.
	Stop(ctx context.Context) error
}
```

When the subscription succeeds it returns a channel that is feed with each new consuming operation
, which contains either a new received message or, an error.     

```go
type Next struct {
	Message ReceivedMessage
	Err     error
}
```

The [received message](message.go#L13) provides an abstraction of the underlying message implementation 
for the pu/sub system.    
  
It has the same data as the envelope plus the `Ack` method to acknowledge the message
once it has been handled.    

You may build any custom logic on top of a subscriber to handle the message live cycle in your own
way, usually it's more useful to use the router to handle it.     

### Router
A router holds a group of subscribers and runs them all together, starting and stopping all of them
at the same time.     

To initialize the router pass the unmarshaller and register all the subscribers along with the
 message handler associated to them.     

Call `Run` on the router; it starts all the subscribers, and if all succeed, it will consume the
messages in an endless loop, feeding them to the handlers after unmarshalling the message body.     

If the handler does not return an error it will acknowledge the message as long as `DisableAutoAck` is false.   

#### Message handler
Register a message handler in the router using the method `Register`.    

You'll need to provide:    
* The topic name     
* The subscriber that consumes the topic messages    
* The handler for the topic message    

The handler receives a message and reports the result of handling it
```go
// Handler handles events.
type Handler interface {
	HandleMessage(ctx context.Context, message *Message) error
}
```

##### Publisher handler
Sometimes a handler will need to publish a new message after handling its own.    

You could publish on the same handler, but if you are using the standard publisher provided
you can use its method `Handler` that accepts handler that return a list of messages
to be published.     

```go
// PublisherHandler handles events and generates new
// messages that will be published.
type PublisherHandler interface {
	HandleMessage(ctx context.Context, message *Message) ([]*Message, error)
}
```

```go
router.Register(
    "incoming-topic",
    subscriber,
    publisher.Handler("outgoing-topic", publisherHandler)
)
```

#### Stopping the router
By default, the router will stop only once the given context terminates, this means that skips 
to the next message if any of the steps fail.     

You can change the behaviour using the different router `Checkpoints` that are invoked on every
step.    

Each checkpoint will receive the topic, the received message, and the result (it will be nil on success).    
You can use them to log and monitor the functioning of the subscriptions.    

If the return value of a checkpoint is not nil, the router will trigger a shutdown stopping **all**
the subscribers and returning the error.    

```go
type Checkpoint func(ctx context.Context, topic string, msg ReceivedMessage, err error) error
```
This is the list of checkpoints available    

* `OnReceive`: after the subscriber provides the next result.    
* `OnUnmarshal`: after the unmarshalling the received message body.    
* `OnHandler`: after the handler returns.    
* `OnAck`: after acknowledging the message.    

##### Stop timeout
By default, the router will trigger the shutdown and wait indefinitely until all the subscribers
stop, then return reporting any errors that may have happened.    

You can use the field `StopTimeout` to set a maximum time to wait for the subscribers to stop.   

### Marshaller/Unmarshaller
The (un)marshaller sits between the pub/sub system encoding and decoding the message data.    

```go
// Marshals the contents of message data.
type Marshaller interface {
	Marshal(data interface{}) (payload []byte, version string, err error)
}

// Unmarshaller will decode the received message.
// It should be aware of the version.
type Unmarshaller interface {
	Unmarshal(topic string, message ReceivedMessage) (*Message, error)
}
```    

The provided un/marshallers are:    
 * JSON: encodes the message data as a JSON string, and decodes into any registered struct either by the event name, or the topic     
 * ProtoText: uses go-proto-sdk v2 to encode data implementing the `proto.Message` interface.    
 * NoOp: accepts either a `string` or a `[]byte` as data payload.    

#### Received Message Version
The version included in the `Envelope/ReceivedMessage` is not the version of data but the version
of the marshaller used to encode this data.    

This is important since having this info helps the unmarshaller to understand if it can decode the payload
or even allows migrating the marshaller in long-lived topic supporting old messages.    

[ci-badge]: https://github.com/hmoragrega/pubsub/workflows/CI/badge.svg
[ci-url]:   https://github.com/hmoragrega/pubsub/actions?query=workflow%3ACI

[coverage-badge]: https://coveralls.io/repos/github/hmoragrega/pubsub/badge.svg?branch=main
[coverage-url]:   https://coveralls.io/github/hmoragrega/pubsub?branch=main

[godoc-badge]: https://pkg.go.dev/badge/github.com/hmoragrega/pubsub.svg
[godoc-url]:   https://pkg.go.dev/github.com/hmoragrega/pubsub

[goreport-badge]: https://goreportcard.com/badge/github.com/hmoragrega/pubsub
[goreport-url]: https://goreportcard.com/report/github.com/hmoragrega/pubsub
