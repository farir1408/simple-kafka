package producer

import (
	"context"

	"github.com/farir1408/simple-kafka/pkg/kafka_tracing"
	"github.com/opentracing/opentracing-go"
	opentracing_log "github.com/opentracing/opentracing-go/log"

	"github.com/farir1408/simple-kafka/pkg/types"

	"github.com/Shopify/sarama"
	"go.uber.org/zap"
)

const (
	defaultRetryMaxCount = 5
)

type Producer struct {
	ctx    context.Context
	cancel context.CancelFunc
	logger *zap.Logger
	cfg    *sarama.Config
	client sarama.AsyncProducer

	enableTracing bool
}

type Option func(p *Producer)

func WIthTracing() Option {
	return func(p *Producer) {
		p.enableTracing = true
	}
}

func New(brokers []string, logger *zap.Logger, opts ...Option) (*Producer, error) {
	ctx, cancel := context.WithCancel(context.Background())
	p := &Producer{
		ctx:           ctx,
		cancel:        cancel,
		logger:        logger,
		enableTracing: false,
	}

	for _, opt := range opts {
		opt(p)
	}

	if p.cfg == nil {
		p.cfg = sarama.NewConfig()
		p.cfg.Producer.RequiredAcks = sarama.WaitForAll
		p.cfg.Producer.Retry.Max = defaultRetryMaxCount
		p.cfg.Producer.Partitioner = sarama.NewRoundRobinPartitioner
	}

	client, err := sarama.NewAsyncProducer(brokers, p.cfg)
	if err != nil {
		return nil, err
	}
	p.client = client
	go p.logErrors()

	p.logger.Info("Produce kafka", zap.Strings("brokers", brokers))
	return p, nil
}

func (p *Producer) SaveMessage(ctx context.Context, topicName string, msg types.Message) {
	// Construct sarama message.
	saramaMsg := &sarama.ProducerMessage{
		Topic: topicName,
		Value: sarama.ByteEncoder(msg.Body),
	}

	// Insert opentracing.Span into message.Headers.
	if p.enableTracing {
		p.insertOpentracingSpan(ctx, saramaMsg)
	}

	select {
	case p.client.Input() <- saramaMsg:
	case <-p.ctx.Done():
		p.logger.Error("ctx done", zap.Error(p.ctx.Err()))
	}
}

func (p *Producer) Close() error {
	p.cancel()
	return p.client.Close()
}

//TODO: сделать прокидывание спана более универсальным, без привязки к библиотеке, возможно через зависимость на некий TraceProvider.
func (p *Producer) insertOpentracingSpan(ctx context.Context, msg *sarama.ProducerMessage) {
	// Get span from ctx, if not span in ctx - create new.
	span, _ := opentracing.StartSpanFromContext(ctx, "producer.kafka.SaveMessage")
	defer span.Finish()
	span.LogFields(opentracing_log.String("topic", msg.Topic))

	// Wrap sarama headers with methods for inserting span headers.
	spanHeaders := kafka_tracing.SaramaTracingHeaders(msg.Headers)
	err := span.Tracer().Inject(span.Context(), opentracing.TextMap, &spanHeaders)
	if err == nil {
		msg.Headers = spanHeaders
	}
}

func (p *Producer) logErrors() {
	for {
		select {
		case err, ok := <-p.client.Errors():
			if ok {
				p.logger.Error("produce message error", zap.Error(err))
			}
		case <-p.ctx.Done():
			return
		}
	}
}
