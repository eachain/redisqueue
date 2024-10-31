# redisqueue

redisqueue参考[模式： 安全的队列](http://doc.redisfans.com/list/rpoplpush.html#id2)，实现了一个多生产者，多消费者的队列。

## 示例

```go
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/eachain/redisqueue"
	"github.com/go-redis/redis/v8"
)

type redisClient redis.Client

func (r *redisClient) LPush(ctx context.Context, queue string, value any) (int64, error) {
	return (*redis.Client)(r).LPush(ctx, queue, value).Result()
}

func (r *redisClient) BRPopLPush(ctx context.Context, src, dst string, timeoutSeconds int64) ([]byte, error) {
	return (*redis.Client)(r).BRPopLPush(ctx, src, dst, time.Duration(timeoutSeconds)*time.Second).Bytes()
}

func (r *redisClient) Del(ctx context.Context, key string) (int64, error) {
	return (*redis.Client)(r).Del(ctx, key).Result()
}

func (r *redisClient) LRange(ctx context.Context, key string, start, stop int64) ([][]byte, error) {
	results, err := (*redis.Client)(r).LRange(ctx, key, start, stop).Result()
	if err != nil {
		return nil, err
	}
	if len(results) == 0 {
		return nil, nil
	}
	bytes := make([][]byte, len(results))
	for i := range bytes {
		bytes[i] = []byte(results[i])
	}
	return bytes, nil
}

func main() {
	var rdb *redis.Client

	rdb = redis.NewClient(&redis.Options{
		Addr:     "127.0.0.1:6379",
		Password: "password",
	})

	queue := "queue:biz:name"
	hostname, err := os.Hostname()
	if err != nil {
		panic(err)
	}

	go func() {
		for i := 1; i <= 10; i++ {
			redisqueue.ProduceJSON((*redisClient)(rdb), context.Background(), queue, i)
		}
	}()

	consumer := redisqueue.NewConsumer((*redisClient)(rdb), queue, queue+":"+hostname, func(ctx context.Context, m *int) error {
		fmt.Printf("consume msg: %v\n", *m)
		return nil
	})
	consumer.Run(context.Background())
}
```
