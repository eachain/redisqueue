package redisqueue

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"
)

type mockRedis struct {
	mut  sync.Mutex
	list map[string][][]byte
}

func (r *mockRedis) LPush(ctx context.Context, queue string, value any) (int64, error) {
	r.mut.Lock()
	defer r.mut.Unlock()
	return r.lpush(queue, value)
}

func (r *mockRedis) lpush(queue string, value any) (int64, error) {
	item, _ := json.Marshal(value)
	if r.list == nil {
		r.list = make(map[string][][]byte)
	}
	r.list[queue] = append(r.list[queue], item)
	return int64(len(r.list[queue])), nil
}

func (r *mockRedis) LPushTrim(ctx context.Context, queue string, value any, trim int64) error {
	r.mut.Lock()
	defer r.mut.Unlock()

	r.lpush(queue, value)
	list := r.list[queue]
	if int64(len(list)) > trim {
		r.list[queue] = list[int64(len(list))-trim:]
	}
	return nil
}

func (r *mockRedis) RPop(ctx context.Context, queue string) ([]byte, error) {
	r.mut.Lock()
	defer r.mut.Unlock()

	list := r.list[queue]
	if len(list) == 0 {
		return nil, ErrRedisNil
	}
	item := list[0]
	if len(list) > 1 {
		r.list[queue] = list[1:]
	} else {
		delete(r.list, queue)
	}
	return item, nil
}

func (r *mockRedis) BRPop(ctx context.Context, key string, timeoutSeconds int64) ([]byte, error) {
	ticker := time.NewTicker(time.Duration(timeoutSeconds) * time.Second / 10)
	defer ticker.Stop()

	for i := 0; i < 10; i++ {
		item, err := r.RPop(ctx, key)
		if err == nil {
			return item, nil
		}
		<-ticker.C
	}
	return r.RPop(ctx, key)
}

func (r *mockRedis) BRPopLPush(ctx context.Context, src, dst string, timeoutSeconds int64) ([]byte, error) {
	ticker := time.NewTicker(time.Duration(timeoutSeconds) * time.Second / 10)
	defer ticker.Stop()

	for i := 0; i < 10; i++ {
		item, err := r.RPopLPush(ctx, src, dst)
		if err == nil {
			return item, nil
		}
		<-ticker.C
	}
	return r.RPopLPush(ctx, src, dst)
}

func (r *mockRedis) RPopLPush(ctx context.Context, src, dst string) ([]byte, error) {
	r.mut.Lock()
	defer r.mut.Unlock()

	list := r.list[src]
	if len(list) == 0 {
		return nil, ErrRedisNil
	}
	item := list[0]
	if len(list) > 1 {
		r.list[src] = list[1:]
	} else {
		delete(r.list, src)
	}
	r.list[dst] = append(r.list[dst], item)
	return item, nil
}

func (r *mockRedis) Del(ctx context.Context, key string) (int64, error) {
	r.mut.Lock()
	defer r.mut.Unlock()

	list := r.list[key]
	delete(r.list, key)
	if len(list) > 0 {
		return 1, nil
	}
	return 0, nil
}

func (r *mockRedis) LRange(ctx context.Context, key string, start, stop int64) ([][]byte, error) {
	r.mut.Lock()
	defer r.mut.Unlock()

	list := r.list[key]
	if len(list) == 0 {
		return nil, nil
	}

	if start < 0 {
		start = int64(len(list)) + int64(len(list))%start
	}
	if stop < 0 {
		stop = int64(len(list)) + int64(len(list))%stop
	}
	stop += 1
	if start == stop {
		return nil, nil
	}
	return list[start:stop], nil
}

func TestProduceConsume(t *testing.T) {
	redis := new(mockRedis)
	value := 1234567890
	Produce(redis, context.Background(), "queue", value)

	var result int
	ConsumeJSON(redis, context.Background(), "queue", &result)
	if result != value {
		t.Fatalf("ConsumeJSON: %v", result)
	}
}

func TestConsumer(t *testing.T) {
	redis := new(mockRedis)
	queue := "queue"

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	read := make(chan struct{})
	write := make(chan struct{})

	go func() {
		defer cancel()
		for i := 0; i < 10; i++ {
			<-write
			Produce(redis, ctx, queue, i)
			read <- struct{}{}
		}
		<-write
	}()

	var results []int
	consumer := NewConsumer(redis, queue, "hostname", func(ctx context.Context, m *int) error {
		<-read
		results = append(results, *m)
		write <- struct{}{}
		return nil
	})
	write <- struct{}{}
	consumer.Run(ctx)

	if len(results) != 10 {
		t.Fatalf("results size: %v", len(results))
	}

	for i, r := range results {
		if r != i {
			t.Fatalf("results[%v] = %v", i, r)
		}
	}
}
