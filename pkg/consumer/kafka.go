package consumer

import (
	"context"
	"io"
	"sync"
	"sync/atomic"

	"go.uber.org/zap"

	"github.com/farir1408/simple-kafka/pkg/types"

	"github.com/Shopify/sarama"
)

const (
	defaultKafkaVersion = "0.11.0.1"
)

// Custom handler for working with sarama.
type InternalHandler interface {
	sarama.ConsumerGroupHandler
	io.Closer

	// For output messages from sarama.
	GetMessages() <-chan types.Message
}

type Consumer struct {
	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	logger  *zap.Logger
	started int32
	group   string
	topic   string
	cfg     *sarama.Config
	client  sarama.ConsumerGroup

	handler InternalHandler
}

type Option func(consumer *Consumer)

func NewConsumer(brokers []string, group, topic string, logger *zap.Logger, opts ...Option) (*Consumer, error) {
	ctx, cancel := context.WithCancel(context.Background())

	handler := NewHandler(group)
	consumer := &Consumer{
		ctx:     ctx,
		cancel:  cancel,
		group:   group,
		handler: handler,
		logger:  logger,
		topic:   topic,
	}

	for _, opt := range opts {
		opt(consumer)
	}

	if consumer.cfg == nil {
		version, err := sarama.ParseKafkaVersion(defaultKafkaVersion)
		if err != nil {
			return nil, err
		}
		consumer.cfg = sarama.NewConfig()
		consumer.cfg.Version = version
	}

	client, err := sarama.NewConsumerGroup(brokers, consumer.group, consumer.cfg)
	if err != nil {
		return nil, err
	}
	consumer.client = client

	return consumer, nil
}

func (c *Consumer) Start() {
	if !atomic.CompareAndSwapInt32(&c.started, 0, 1) {
		c.logger.Error("already started")
		return
	}
	defer atomic.StoreInt32(&c.started, 0)

	c.wg.Add(1)
	c.logger.Info("start consumer...")
	go func() {
		defer c.wg.Done()
		for {
			if c.ctx.Err() == context.Canceled {
				return
			}
			err := c.client.Consume(c.ctx, []string{c.topic}, c.handler)
			if err != nil {
				c.logger.Error("consumer done failed", zap.Error(err))
			}
		}
	}()
}

func (c *Consumer) Close() error {
	c.cancel()
	c.client.Close()
	c.wg.Wait()
	return c.handler.Close()
}

func (c *Consumer) GetMessages() <-chan types.Message {
	return c.handler.GetMessages()
}
