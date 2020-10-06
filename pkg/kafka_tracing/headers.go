package kafka_tracing

import (
	"bytes"

	"github.com/Shopify/sarama"
)

type SaramaTracingHeaders []sarama.RecordHeader

// ForeachKey conforms to the opentracing.TextMapReader interface.
func (c SaramaTracingHeaders) ForeachKey(handler func(key, val string) error) error {
	for idx := range c {
		if err := handler(string(c[idx].Key), string(c[idx].Value)); err != nil {
			return err
		}
	}
	return nil
}

// Set implements Set() of opentracing.TextMapWriter
func (c SaramaTracingHeaders) Set(key, val string) {
	for idx := range c {
		if bytes.Equal(c[idx].Key, []byte(key)) {
			c[idx].Key = []byte(val)
			return
		}
	}

	c = append(c, sarama.RecordHeader{
		Key:   []byte(key),
		Value: []byte(val),
	})
}
