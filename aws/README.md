# AWS SNS+SQS

[![ci][ci-badge]][ci-url]

Implementation for publishing messages with SNS and SQS using [v1 of the Go SDK](https://github.com/aws/aws-sdk-go) for AWS.

## Publisher
We use SQS to publish messages to a topic ARN, so to be able to send messages the
publisher needs to translate the general topic name to its ARN.

```go
sqsPublisher := aws.Publisher{
    SNS: snsService,
    TopicARNs: map[string]string{
        "topic-one": "arn:aws:sns:us-east-2:444455556666:topic-one"
        "topic-two": "arn:aws:sns:us-east-2:444455556666:topic-two"
    },
}
```

Note: it can publish directly to topics starting with `arn:aws:sns`  

## Queue Subscriber
We use SQS to consume messages from a queue, using its queue URL:
```go
snsSubscriber := aws.Subscriber{
    SQS: sqsService,
    QueueURL: "https://sqs.us-east-2.amazonaws.com/123456789012/MyQueue"
}
```
The subscriber will use [long polling](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-short-and-long-polling.html)
for 20s max, and will get a batch of 10 messages on each consumption,
although it will feed the messages one by one to the subscription channel.

Once the message has been handled it will be acknowledged one by one deleting the message.

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

In this mode, the errors that may happen acknowledging a message will be delivered to the next call 
to acknowledge a message but the message will be added to the next batch anyways. Also, when the 
subscriber stops, it will wait until all the pending acknowledgements are flushed, and the possible 
errors returned as the result of the `Stop` method.

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
subscriptionARN, err := aws.Subscribe(ctx, snsService, topicARN, queueARN)
err := AttachQueueForwardingPolicy(ctx, sqsService, queueURL, queueARN, topicARN)
```

### Raw Delivery
By the default SNS wraps the message in a notification object to deliver it to an SQS queue, 
we need to enable the option `RawMessageDelivery` when subscribing a queue to a topic to prevent
this.

In general this package provides some helper to bootstrap creating new topics and queues and subscribing
 them with the necessary options to work with the parent pubsub package.   

[ci-badge]: https://github.com/hmoragrega/pubsub/workflows/AWS/badge.svg
[ci-url]:   https://github.com/hmoragrega/pubsub/actions?query=workflow%3AAWS
