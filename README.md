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
* **Name:** (optional) name for the message.
* **Key:** (optional) certain pub/sub system can us the key to provide FIFO semantics to
messages with the same key within a single topic.
* **Attributes:** (optional) a `map[string]string` with even custom metadata.

### Publisher
The publisher sends the message to a given topic using the underlying pub/sub system implementation.

```
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
