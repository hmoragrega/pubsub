# AWS SNS+SQS

[![ci][ci-badge]][ci-url]

Implementation for publishing messages with SNS and SQS using [v1 of the Go SDK](https://github.com/aws/aws-sdk-go) for
AWS.

## Publisher

There are two available publishers for AWS

* SNS Publisher: publishes to a SNS topic.
* SQS Publisher: publishes to directly to a SQS queue.

### SNS Publisher

We use SNS to publish messages to a topic ARN.

The publisher accepts a map of topic names (or alias) to its AWS topic ARN counterpart. This allows to decouple the
application topic names from AWS, although it also supports publishing directly to an AWS topic ARN.

```go
snsPublisher := aws.NewSNSPublisher(
    snsClient,
    map[string]string{
        "topic-one": "arn:aws:sns:us-east-2:444455556666:topic-one",
        "topic-two": "arn:aws:sns:us-east-2:444455556666:topic-two",
    }
)

// These are equivalent
snsPublisher.Publish(ctx, "topic-one", msg)
snsPublisher.Publish(ctx, "arn:aws:sns:us-east-2:444455556666:topic-one", msg)
```

### SQS Publisher

Publishes directly to a single SQS queue.

The publisher accepts a map of queue names (or alias) to its AWS queue URL ARN counterpart. This allows to decouple the
application queue names from AWS, although it also supports publishing directly to an queue URL if a valid URL is given.

```go
sqsPublisher := aws.NewSQSPublisher(
    snsClient,
    map[string]string{
        "queue-one": "https://sqs.eu-west-3.amazonaws.com/444455556666/queue-one",
        "queue-two": "https://sqs.eu-west-3.amazonaws.com/444455556666/queue-two",
    }
)

// These are equivalent
sqsPublisher.Publish(ctx, "queue-one", msg)
sqsPublisher.Publish(ctx, "https://sqs.eu-west-3.amazonaws.com/444455556666/queue-one", msg)
```

There is a helper constructor `NewSQSDirectPublisher` that can be used if no mapping is necessary

```go
// These initializations are equivalent
aws.NewSQSDirectPublisher(snsClient)
aws.NewSQSPublisher(snsClient, map[string]string{})
```

#### Scheduling
When using a `pubsub.Scheduler` in combination with the SQS publisher you can leverage the normal
message delay that SQS allows, in this way only scheduled messages with a due date than more than
15 minutes will be sent to the scheduler storage, but published instead.

You can use the `SQSSchedulerStorage` as both `EnvelopePubliser` and `SchedulerStorage`

```go
dbStorage := storage.NewPostgres("instanceID", "table", dbConn)
sqsPub := NewSQSDirectPublisher(sqsTest)

// With this storage, messages with delay < 15 min
// will be directly published to SQS with the proper delay.
// Otherwise, they will be stored in the database. 
s := pubsub.NewSQSSchedulerStorage(sqsPub, dbStorage)

// build the final scheduler/publisher
scheduler := pubsub.NewSchedulerPublisher(pubsub.NewPublisher(sqsPub, marshaller), sqsStor)

// publish message with 5 minutes delay. 
scheduler.Delay(ctx, 5*time.Minutes, queuURL, message)
```

## Queue Subscriber

We use SQS to consume messages from a queue, using its queue URL:

```go
snsSubscriber := aws.NewSQSSubscriber(
    sqsClient,
    "https://sqs.us-east-2.amazonaws.com/123456789012/MyQueue"
)
```
The subscriber will use [long polling](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-short-and-long-polling.html)
for 20s max, and will get a batch of 10 messages on each consumption,
although it will feed the messages one by one to the subscription channel.

These parameters can be tweaked when initializing the subscriber

### Maximum number of messages
You can use the optional parameter function `WithMaxMessages` to change the maximum number of message for every
receive request. Valid values are from 1 to 10.

### Message Visibility Timeout
By default, the queue message visibility will be used. You can use the optional parameter function
 `WithVisibilityTimeout` to change it for a single subscriber. Valid values are from 1s to 43200s (12 hours).

### Receiving Messages Wait Time
You can tweak the long-poling mechanism or disable it completely using `WithWaitTime`. Valid values are from 0 to 20s

### Acknowledgement wait time.
After receiving a new batch of messages the subscriber will pause until all the messages are acknowledged (either with `ack` or `nack`),
This ensures that the new batch will have the full visibility window available, It could occur though, that some message
is not acknowledged, blocking the subscriber indefinitely. To prevent this situation the subscriber will request a new batch
if the "acknowledgement wait time" expires, by default this value is 30s, but ideally this value should be greater than the queue or the
subscriber visibility timeout. You can tweak the value using `WithAckWaitTime`.

### Batch & Async acknowledgement
We can enable batching the acknowledgements, this will reduce the number of requests to SQS, also,
speed up the consumption of the next message.   

To do you can tweak the [AckConfig](subscriber.go#L43) in the subscriber:
* `Async`: if set true it will acknowledge the messages asynchronously.   
* `BatchSize`: set a value greater than 0 to use batch acknowledgements, enabling batching will enable asynchronous
 acknowledgements automatically.   
* `FlushEvery`: use it to force a flush of the pending acknowledgements after certain time since the last
batch. This is key for very low frequency topics, given that SNS re-delivers messages that have not been acknowledged
after certain amount of time (30s by default).
* `ChangeVisibilityOnNack`: by default nothing is done when a message is negatively acknowledged, you can set this flag to 
true to force setting the message visibility to zero, which will indicate AWS that the message is can be delivered again.
Please note that this mode is not compatible with asynchronous acknowledgements, and the subscriber initialization will trigger an error.

In this mode, the errors that may happen acknowledging a message will be delivered to the next call 
to acknowledge a message but the message will be added to the next batch anyways. Also, when the 
subscriber stops, it will wait until all the pending acknowledgements are flushed, and the possible 
errors returned as the result of the `Stop` method.

### Dead Letter Queue


## SNS+SQS integration
It's worth noting some gotchas while working with SNS+SQS

### Message constraints
There are certain [constraints imposed to the messages](https://docs.aws.amazon.com/sdk-for-go/api/service/sns/#PublishInput)
that could lead to publishing errors, the most important one is the message body is sent as a
string using an HTTP request, so there is a limitation in the character set that is supported.

It is advised to use a marshaller that encodes the binary payload within the supported set. 
For example the `JSONMarshaller` or the `ProtoTextMarshaller`. Please note that this limitation
 also applies to the message attributes.  

### Allow SNS to fan out to SQS queues
After subscribing a SQS queue to a SNS topic, you still need to set the correct queue policy to allow
the topic to send messages to the queue

This package provides a helper that will a single topic to publish to a single queue
```go
subscriptionARN, err := aws.Subscribe(ctx, snsClient, topicARN, queueARN)
err := aws.AttachQueueForwardingPolicy(ctx, sqsClient, queueURL, queueARN, topicARN)
```

### Raw Delivery

In general this package provides some helper to bootstrap creating new topics and queues and subscribing
 them with the necessary options to work with the parent pubsub package.   

[ci-badge]: https://github.com/hmoragrega/pubsub/workflows/CI/badge.svg
[ci-url]:   https://github.com/hmoragrega/pubsub/actions?query=workflow%3ACI
