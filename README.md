# Pub/Sub

Package for publishing and consuming messages from pub/sub systems.

Supported:
 * AWS (SNS+SQS)

## TL;DR
Publish a message
```go
publisher := pubsub.Publisher{
	Publisher:  awsPublisher,
	Marshaller: &marshaller.ProtoTextMarshaller{},
}

publisher.Publish(ctx, "some-topic", &pubsub.Message{
    Data: &proto.SomeMessage {
    	Foo: "bar",
    },
})
```

Consume messages
```go
var unmarshaller marshaller.ProtoTextMarshaller
unmarshaller.Register("some-topic", &proto.SomeMessage{})

router := pubsub.Router {
	Unmarshaller: &unmarshaller,
}

router.Register(
    "some-topic",
    awsQueueSusbcriber,
    pubsub.HandlerFunc(func(ctx context.Context, message *pubsub.Message) error {
        msg := request.Data.(*proto.SomeMessage)
        fmt.Println(msg.Foo)
    })
)

router.Run(ctx)
```

## Components
### Message
The key component in the flow is the [message](message.go#L18), this is the struct that 
is going to be published, and the struct that your handlers will receive.     

The message contains this information:    
* **Data:** it accepts any type (accepts `interface{}`), as long as the marshaller
you've chosen supports serializing it to a `[]byte`.     
* **ID:** (optional) a `string` with an ID for the message, if not provided, the publisher
 will add one UUIDv4 automatically.     
* **Name:** (optional) name for the message, it can be used to [dispatch the messages](handler.go#L25) using the name as 
discriminator while sharing the same topic.     
* **Key:** (optional) certain pub/sub system can leverage the key to provide FIFO semantics to
messages with the same key within a single topic.      
* **Attributes:** (optional) a `map[string]string` with even custom metadata.

### Publisher
The publisher sends the message to a given topic using the underlying pub/sub system implementation.    

```go
type Publisher interface {
	Publish(ctx context.Context, topic string, message Message) error
}
``` 

Steps:
* Serializes the message data into using the marshaller
* Sends the data, and the rest of the message fields
as an `EnvelopeMessage` to the pub/sub system.     

#### Envelope publisher
This is the specific sender for each pub/sub system, it receives an [envelope](publisher.go#L8), that holds
the data for the message (this time as `[]byte`), the envelope has also the version of the marshaller 
used to serialize the data.    

you can check the implementation for Amazon's [SNS service here.](aws/publisher.go#L22)     

### Subscriber
The subscriber is can subscribe to a single topic in a pub/sub system and consume the 
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

`Subscribe` method tries to subscribe to the topic, if it succeeds it returns a channel
that is feed with each new consuming operation, which contain either a new received message
or, an error.     

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

Call `Run` on the router; it subscribes all the subscribes, if all succeed wit will consume the
messages in an endless loop, feeding the messages to the handler after unmarshalling the data.     

If the handler does not return an error it will acknowledge the message as long as `DisableAutoAck`.   

#### Message handler
Register a message handler in the router using the method `Register`. You'll need to provide:    
* The topic name     
* The subscriber that consumer the topic messages    
* The handler for the topic message    

The handler receives a message and reports the result of handling it
```go
// Handler handles events.
type Handler interface {
	HandleMessage(ctx context.Context, message *Message) error
}
```

##### Publisher handler
Sometimes a handler will need to publish a new message after handling its own,
you can do it own the same handler, but if you are using the standard publisher provided
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
By default, the router will stop only once the context given terminates, this means that skips 
to the next message if any of the steps fail.     

You can change the behaviour using the different router `Checkpoints` that are invoked on every
step.    

Each checkpoint will contain the topic, the received message, and the result (it will be nil on success)
You can use them to log and monitor the functioning of the subscriptions.    

If the return value of a checkpoint is not nil, the router will trigger a shutdown stopping **all**
the subscribers and returning the error    

```go
type Checkpoint func(ctx context.Context, topic string, msg ReceivedMessage, err error) error
```
This is the list of checkpoints available    

* `OnReceive`: it is call after the subscriber provides the next result.    
* `OnUnmarshal`: it is call after the unmarshalling the received message body.    
* `OnHandler`: it is call after the handler returns.    
* `OnAck`: it is call with the result of the message acknowledgement.    

##### Stop timeout
By default, the router will trigger the shutdown and wait indefinitely until all the subscriptions
stop, then return reporting any errors that may have happened.    

You can use the field `StopTimeout` to set a maximum time to wait for the subscribers to stop.   

### Marshaller/Unmarshaller
The (un)marshaller sits between the pub/sub system encoding and decoding the messages to abstract.    

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

The provided un/marshaller are:    
 * JSON: encodes the message data as a JSON string     
 * ProtoText: uses go proto v2 to encode `proto.Message`.    
 * NoOp: accepts either a `string` or a `[]byte` as data payload.    

#### Received Message Version
The version included in the `Envelope/ReceivedMessage` is not the version of data but the version
of the marshaller used to encode this data.    

This is important since this the unmarshaller in the router can check if the message was marshalled 
with the proper marshaller, or being able to migrate the marshaller message while still supporting 
the old messages in a long-lived topic.    
