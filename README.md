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
* **Attributes:** (optional) a `map[string]string` with custom message metadata.

### Publisher
The publisher sends the message to a given topic using the underlying pub/sub system implementation.    

```go
type Publisher interface {
	Publish(ctx context.Context, topic string, messages ...*Message) error
}
``` 

Steps:
* Serializes the message data using the marshaller
* Sends the data, and the rest of the message fields
as an `EnvelopeMessage` to the pub/sub system.     

### Scheduler
The scheduler supersedes a normal Publisher as it can schedule a message to be published at any time in the future

**NOTE** You need to provide some infrastructure to store the messages while they are not due. 

```go
type Scheduler interface {
    Publisher
    Schedule(ctx context.Context, dueDate time.Time, topic string, messages ...*Message) error
    Delay(ctx context.Context, delay time.Duration, topic string, messages ...*Message) error
}
``` 

You can use the `Schedule` method when you want to publish a message at some concrete time, or `Delay` 
to indicate that the message should be sent after some duration.  

#### Publishing pending messages
You'll need a background process to publish the due pending messages. Instantiate the scheduler and
call `PublishDue`  

```go
import (
    "github.com/hmoragrega/pubsub"
    "github.com/hmoragrega/pubsub/schedule/storage/postgres"
)

// External storage to store the pending messages.
storage := postgres.NewPostgres("instance-123", "pending_messages", dbClient)

p := pubsub.NewSchedulerPublisher(marshaller, storage)

// Sends the due messages in a loop blocking until context
// is terminated or an error occurs.
if err := p.PublishDue(ctx); err != nil {
	log.Fatal("error happened while published pending messages: %w", err)
}
```

#### Pending storage

The scheduler requires some infrastructure to store the messages while they are not due, PostgresQL is
the only implementation provided, but you can provide yours with this interface

```go
type SchedulerStorage interface {
	// Schedule schedules a message to be published in the future.
	Schedule(ctx context.Context, dueDate time.Time, topic string, messages ...*Envelope) error

	// ConsumeDue returns a channel that should receive the next due messages or an error.
	// It should feed the next messages while the context is valid, once the context terminates
	// it should make any cleaning task and close the channel to signal a proper cleanup.
	ConsumeDue(ctx context.Context) (<-chan DueMessage, error)

	// Published indicates to the storage that the message has been published.
	Published(ctx context.Context, message DueMessage) error
}
```

##### Postgres storage
It stores the messages in a table, you need to create such table and pass the name while instantiating it.

This is the recommended table, messages are encoded as JSON by default.
```sql
CREATE TABLE IF NOT EXISTS pending_messages (
    id               BIGSERIAL PRIMARY KEY,
    message_id       TEXT NOT NULL,
    due_date         TIMESTAMP WITH TIME ZONE NOT NULL,
    topic            TEXT NOT NULL,
    message          BYTEA NOT NULL, -- JSONB works too
    instance_id      TEXT DEFAULT NULL, 
    instance_timeout TIMESTAMP WITH TIME ZONE DEFAULT NULL,

    CONSTRAINT message_id_idx UNIQUE (message_id)
```

If the number of pending messages can grow large, it is recommended to have some indexes to improve the performance
```sql
-- Used for selecting the next due messages to process 
CREATE INDEX pending_messages_next_batch_idx
    ON pending_messages (due_date, instance_id);
```

**Concurrency and ordering:**

 It is fine to have multiple postgres storages retrieving messages to be sent, they lock 
their next batch, trying to respect the inserting order after they are due, **but** since 
they work in batches, they will start feeding the messages concurrently to the parent process,
so they will be published unordered.   

If your application **needs** them to be published in insertion order you *must* have only
one process retrieving the due messages. 

**Fail over:**
When a batch of messages is locked for publishing, the instance sets a timeout, if the timeout
is reached and the messages have not yet been deleted from the table, another instance will
pick them and process them, so if an instance crashes or blocks, those messages can be sent.  

It can happen that if the timeout is reached but the instance was working fine (just slower), 
another instance can pick the messages, and they would be duplicated twice. This is a compromise as this
system as been design with "at least one" publishing.

If your system needs "exactly one" publishing you **must** have only one instance processing pending
messages at the cost of lower throughput.

**Clean Up:**
When the consuming context is terminated, the storage will block before stopping until all
it's current batch pending messages are published, this helps to prevent those
locked messages to have to wait until the instance fail over before another instance picks them up.

Keep this in mind before killing application forcibly, give the kill signal enough time
to publish the batch according to it's size.  

**Options:**
Most apps can work with the default values, but you can tweak the behavior explained before
with the options.

```go
type Options struct {
	// MinWaitTime minimum wait time between that will wait before processing the next batch of due messages.
	// If the batch took more to be processed, the next one will be processed immediately.
	// On mostly empty tables a longer time will prevent hitting the DB constantly.
	// at the cost of losing some precision in the due date.
	// Default: 500ms.
	MinWaitTime time.Duration

	// BatchSize number of messages that will be processed at the same time.
	// Default: 100.
	BatchSize int

	// InstanceTimeout specifies the time that a message will be locked for publishing after 
	// has been selected for this instance. After this time, the message will be available again.
	// A reasonable value should allow you app to publish all the messages in the batch.
	// Default: 1 minute.
	InstanceTimeout time.Duration

	// Encoder can be used to customize how to encode the messages for storing.
	// Default: JSON encoder
	Encoder encoder
}
```

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

When the subscription succeeds it returns a channel that is fed with each new consuming operation
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
 
**NOTE:** If you are using an enveloper publsiher and/or marshalling in your own terms, the router
will use the `NoOpUnmarshaller` function wich will use the raw data `[]byte` slice for the message.

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
