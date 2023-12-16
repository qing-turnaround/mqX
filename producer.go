package mqX

import (
	"context"
	"github.com/qing-turnaround/mqX/redis"
)

type Producer struct {
	client *redis.Client
	opts   *ProducerOptions
}

func NewProducer(client *redis.Client, opts ...ProducerOption) *Producer {
	p := Producer{
		client: client,
		opts:   &ProducerOptions{},
	}

	for _, opt := range opts {
		opt(p.opts)
	}

	repairProducer(p.opts)

	return &p
}

// SendMsg 生产一条消息
func (p *Producer) SendMsg(ctx context.Context, topic, msg string) (string, error) {
	return p.client.XADD(ctx, topic, p.opts.msgQueueLen, msg)
}
