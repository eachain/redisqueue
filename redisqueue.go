// queue通过redis list结构实现一个简单的消息队列。
// 该消息队列允许多个生产者和多个消费者。
// 队列中的某条消息被有且仅有一个消费者消费掉。

package redisqueue

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"time"
)

type RedisProducerClient interface {
	LPush(ctx context.Context, queue string, value any) (int64, error)
}

// Produce投递一条新消息到消息队列。
func Produce(cli RedisProducerClient, ctx context.Context, queue string, value any) error {
	_, err := cli.LPush(ctx, queue, value)
	return err
}

// ProduceJSON投递一条新json消息到消息队列。
func ProduceJSON(cli RedisProducerClient, ctx context.Context, queue string, value any) error {
	item, err := json.Marshal(value)
	if err != nil {
		return err
	}
	_, err = cli.LPush(ctx, queue, item)
	return err
}

type RedisProduceTrimerClient interface {
	// LPushTrim exec redis LPush and LTrim cmd.
	LPushTrim(ctx context.Context, queue string, value any, trim int64) error
}

// ProduceTrim投递一条新消息到消息队列，并只保留最近trim条消息。
func ProduceTrim(cli RedisProduceTrimerClient, ctx context.Context, queue string, value any, trim int64) error {
	return cli.LPushTrim(ctx, queue, value, trim)
}

// ProduceTrimJSON投递一条新json消息到消息队列，并只保留最近trim条消息。
func ProduceTrimJSON(cli RedisProduceTrimerClient, ctx context.Context, queue string, value any, trim int64) error {
	item, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return cli.LPushTrim(ctx, queue, item, trim)
}

type RedisConsumeClient interface {
	RPop(ctx context.Context, queue string) ([]byte, error)
}

// Consume消费一条消息，返回消息内容。
func Consume(cli RedisConsumeClient, ctx context.Context, queue string) ([]byte, error) {
	return cli.RPop(ctx, queue)
}

// ConsumeJSON消费一条json消息，将json反序列化到value中，并返回消息内容。
func ConsumeJSON(cli RedisConsumeClient, ctx context.Context, queue string, value any) ([]byte, error) {
	item, err := cli.RPop(ctx, queue)
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(item, value)
	return item, err
}

type RedisBlockConsumeClient interface {
	BRPop(ctx context.Context, queue string, timeoutSeconds int64) ([]byte, error)
}

// BlockConsume阻塞式消费一条消息，返回消息内容。
func BlockConsume(cli RedisBlockConsumeClient, ctx context.Context, queue string, timeoutSeconds int64) ([]byte, error) {
	return cli.BRPop(ctx, queue, timeoutSeconds)
}

// BlockConsumeJSON阻塞式消费一条json消息，将json反序列化到value中，并返回消息内容。
func BlockConsumeJSON(cli RedisBlockConsumeClient, ctx context.Context, queue string, timeoutSeconds int64, value any) ([]byte, error) {
	item, err := cli.BRPop(ctx, queue, timeoutSeconds)
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(item, value)
	return item, err
}

var ErrRedisNil = errors.New("redis nil")

type RedisConsumerClient interface {
	// BRPopLPush should return ErrRedisNil when no element pop.
	BRPopLPush(ctx context.Context, src, dst string, timeoutSeconds int64) ([]byte, error)
	Del(ctx context.Context, key string) (int64, error)
	LRange(ctx context.Context, key string, start, stop int64) ([][]byte, error)
}

// Consumer 处理消息队列中的消息。
// 一般用于处理重要的、不可丢弃的消息。
// 如果是一般不重要、可丢弃消息，可使用Consume。
type Consumer[Msg any] struct {
	cli     RedisConsumerClient
	queue   string // msg queue
	uniq    string // global uniq backup key for this consumer
	timeout int64  // read timeout for BRPOPLPUSH

	delay time.Duration // sleep on error

	limit int                                 // BRPOPLPUSH times
	batch func(context.Context, []*Msg) error // msg processor for batch consumer
	every func(context.Context, *Msg) error   // msg processor for consumer
}

// NewConsumer args:
// cli: redis client;
// queue: 消息队列;
// uniq: 每个Consumer唯一标识，用于确保消息消费成功，需要保证进程重启后，和之前标识相同;
// process: 消费函数，当返回error时，Consumer会重试，如果没有error返回，标记消息成功消费;
// 由于服务可能重启，网络失败等因素影响，要求业务消费函数幂等。
func NewConsumer[Msg any](cli RedisConsumerClient, queue, uniq string, process func(context.Context, *Msg) error) *Consumer[Msg] {
	c := &Consumer[Msg]{
		cli:     cli,
		queue:   queue,
		uniq:    uniq,
		timeout: 2,
		limit:   1,
		every:   process,
	}
	return c
}

// NewBatchConsumer args:
// cli: redis client;
// queue: 消息队列;
// uniq: 每个Consumer唯一标识，用于确保消息消费成功，需要保证进程重启后，和之前标识相同;
// batch: 批量消费消息数，实际消费一批消息时，消息数量<=batch;
// process: 消费函数，当返回error时，Consumer会重试，如果没有error返回，标记消息成功消费;
// 由于服务可能重启，网络失败等因素影响，要求业务消费函数幂等。
func NewBatchConsumer[Msg any](cli RedisConsumerClient, queue, uniq string, batch int, process func(context.Context, []*Msg) error) *Consumer[Msg] {
	if batch <= 0 {
		batch = 100
	}
	c := &Consumer[Msg]{
		cli:     cli,
		queue:   queue,
		uniq:    uniq,
		timeout: 2,
		limit:   batch,
		batch:   process,
	}
	return c
}

// SetBlockTimeout设置redis命令BRPopLPush阻塞秒数
func (c *Consumer[Msg]) SetBlockTimeout(seconds int64) {
	if seconds > 0 {
		c.timeout = seconds
	}
}

func done(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return true
	default:
		return false
	}
}

// Run启动Consumer，开始消费消息队列。
func (c *Consumer[Msg]) Run(ctx context.Context) {
	// 服务启动时，先将上次正在做的任务完成
	c.redo(ctx)
	c.listen(ctx)
}

func (c *Consumer[Msg]) sleep(ctx context.Context) {
	if c.delay == 0 {
		c.delay = 100 * time.Millisecond
	} else {
		c.delay *= 2
	}
	if max := 5 * time.Second; c.delay > max {
		c.delay = max
	}
	select {
	case <-time.After(c.delay):
	case <-ctx.Done():
	}
}

func (c *Consumer[Msg]) wake() {
	c.delay = 0
}

func (c *Consumer[Msg]) redo(ctx context.Context) {
	msgs := c.pull(ctx)
	if len(msgs) > 0 {
		c.process(ctx, msgs)
		c.ack(ctx)
	}
}

func (c *Consumer[Msg]) process(ctx context.Context, msgs []*Msg) {
	if c.every != nil {
		for _, msg := range msgs {
			for !done(ctx) {
				err := c.every(ctx, msg)
				if err != nil {
					c.sleep(ctx)
					continue
				}
				c.wake()
				break
			}
		}
	} else {
		for !done(ctx) {
			err := c.batch(ctx, msgs)
			if err != nil {
				c.sleep(ctx)
				continue
			}
			c.wake()
			break
		}
	}
}

func (c *Consumer[Msg]) pull(ctx context.Context) []*Msg {
	for !done(ctx) {
		items, err := c.cli.LRange(ctx, c.uniq, 0, -1)
		if err != nil {
			c.sleep(ctx)
			continue
		}
		c.wake()

		if len(items) == 0 {
			return nil
		}
		slices.Reverse(items)

		msgs := make([]*Msg, 0, len(items))
		for _, item := range items {
			msg := new(Msg)
			err := json.Unmarshal(item, msg)
			if err != nil {
				continue
			}
			msgs = append(msgs, msg)
		}
		return msgs
	}

	return nil
}

func (c *Consumer[Msg]) ack(ctx context.Context) {
	for !done(ctx) {
		_, err := c.cli.Del(ctx, c.uniq)
		if err != nil {
			c.sleep(ctx)
			continue
		}
		c.wake()
		break
	}
}

func (c *Consumer[Msg]) listen(ctx context.Context) {
	for !done(ctx) {
		msgs, err := c.pick(ctx)
		if err != nil {
			c.redo(ctx)
		} else if len(msgs) > 0 {
			c.process(ctx, msgs)
			c.ack(ctx)
		}
	}
}

func (c *Consumer[Msg]) pick(ctx context.Context) ([]*Msg, error) {
	var retErr error
	msgs := make([]*Msg, 0, c.limit)
	for i := 0; i < c.limit; i++ {
		if done(ctx) {
			break
		}

		item, err := c.cli.BRPopLPush(ctx, c.queue, c.uniq, c.timeout)
		if err != nil {
			if !errors.Is(err, ErrRedisNil) {
				// 如果是网络等错误，有可能在redis server端已经将event放入working队列
				retErr = err
			}
			continue
		}
		msg := new(Msg)
		err = json.Unmarshal(item, msg)
		if err != nil {
			// retErr = err
			continue
		}
		msgs = append(msgs, msg)
	}

	if retErr != nil {
		return nil, retErr
	}
	return msgs, nil
}
