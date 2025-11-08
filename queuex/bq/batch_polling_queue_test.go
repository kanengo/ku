package bq

import (
	"context"
	"fmt"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/kanengo/ku/convertx"
	"github.com/redis/go-redis/v9"
)

func TestBpq(t *testing.T) {
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

	bdq, _ := NewBatchPollingQueue(context.Background(), "test", rdb, WithConcurrency(4), WithShardNum(4))

	for i := range 100 {
		s := S{
			Id:   100 + int64(i),
			Name: "leeka",
		}
		bdq.Enqueue(context.Background(), convertx.Struct2JsonString(s).Must())

	}

	bdq.Poll(context.Background(), func(ctx context.Context, tasks []Task) []string {
		fmt.Println(tasks)
		var retryList []string
		for _, t := range tasks {
			w := rand.IntN(10000)
			if w > 7000 {
				retryList = append(retryList, t.Id)
			}
		}

		return retryList
	})

	time.Sleep(time.Second * 5)

	bdq.Shutdown()

	fmt.Println("finish")
}
