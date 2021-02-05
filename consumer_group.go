package pubsub

import (
	"context"
	"fmt"
	"sync"
)

// ConsumerGroup groups consumer so all can started
// and stopped at the same time.
type ConsumerGroup struct {
	Consumers []*Consumer
	started   []*Consumer
}

// MustStart will panic if cannot start all the consumers.
// In case of failure, before panicking it will attempt to
// stop clean those consumers that started.
func (cg *ConsumerGroup) MustStartAll(ctx context.Context) {
	_, errs := cg.StartAll()
	if len(errs) == 0 {
		return
	}
	_ = cg.Stop(context.Background())
	panic(fmt.Errorf("failed to start all consumers: %+v", errs))
}

// Start will start all the consumers in the group.
func (cg *ConsumerGroup) StartAll() (started int, errs []error) {
	type result struct {
		c   *Consumer
		err error
	}
	var (
		consumers = cg.Consumers
		n         = len(consumers)
		results   = make(chan result, n)
	)
	for _, c := range consumers {
		go func(c *Consumer) {
			err := c.Start()
			results <- result{c: c, err: err}
		}(c)
	}
	for i := 0; i < n; i++ {
		res := <-results
		if res.err != nil {
			errs = append(errs, res.err)
			continue
		}
		cg.started = append(cg.started, res.c)
	}
	return len(cg.started), errs
}

// Stop will stop the consumer that started successfully
func (cg *ConsumerGroup) Stop(ctx context.Context) (errs []error) {
	n := len(cg.started)
	results := make(chan error, n)
	for _, c := range cg.started {
		go func(c *Consumer) {
			results <- c.Stop(ctx)
		}(c)
	}
	for i := 0; i < n; i++ {
		if err := <-results; err != nil {
			errs = append(errs, err)
		}
	}
	return errs
}

// Consume starts consuming message until the context
// is terminated.
func (cg *ConsumerGroup) Consume(ctx context.Context) {
	var wg sync.WaitGroup
	for _, c := range cg.started {
		wg.Add(1)
		go func(c *Consumer) {
			c.Consume(ctx)
			wg.Done()
		}(c)
	}
	<-ctx.Done()
	wg.Wait()
}
