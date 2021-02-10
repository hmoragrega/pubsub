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

router.RegisterHandler(
    "some-topic",
    awsQueueSusbcriber,
    pubsub.MessageHandlerFunc(func(ctx context.Context, message *pubsub.Message) error {
        msg := request.Data.(*proto.SomeMessage)
        fmt.Println(msg.Foo)
    })
)

router.Run(ctx)
```
