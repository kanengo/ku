package dq

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/kanengo/ku/queuex"
	"github.com/redis/go-redis/v9"
)

func TestBdq(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "redis-test-0",
		DB:       0,
	})

	if err := rdb.Ping(context.Background()).Err(); err != nil {
		t.Fatal(err)
	}

	type S struct {
		Id   int64
		Name string
	}

	bdq := NewBatchDelayQueue[queuex.JsonMarshaler[S]](context.Background(), "test", rdb, WithConcurrency(4), WithShardNum(4))

	bdq.Enqueue(context.Background(), DelayTask[queuex.JsonMarshaler[S]]{
		ProcessAt: time.Now().Add(time.Second).Unix(),
		Data: queuex.NewJsonMarshaler(S{
			Id:   100,
			Name: "leeka",
		}),
	})

	bdq.Poll(context.Background(), func(ctx context.Context, tasks []DelayTask[queuex.JsonMarshaler[S]]) []string {
		fmt.Println(tasks)
		// for i := range 100 {
		// 	bdq.Enqueue(context.Background(), DelayTask[queuex.JsonMarshaler[S]]{
		// 		ProcessAt: time.Now().Add(time.Second).Unix(),
		// 		Data: queuex.NewJsonMarshaler(S{
		// 			Id:   100 + int64(i),
		// 			Name: "leeka",
		// 		}),
		// 	})
		// }

		return nil
	})

	time.Sleep(time.Second * 10)

	bdq.Shutdown()
}
