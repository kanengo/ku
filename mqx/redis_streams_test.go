package mqx

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

func TestRedisPubSubPublishSubscribe(t *testing.T) {
	ctx := context.Background()
	rdb := newTestRedisClient(t)

	topic := testStreamName("pubsub-basic")
	group := testStreamName("group")

	ps := NewRedisPubSub(rdb,
		WithBlockTimeout(50*time.Millisecond),
		WithReadCount(8),
		WithConcurrency(4),
		WithProcessingTimeout(200*time.Millisecond),
		WithRedeliverInterval(50*time.Millisecond),
	)
	t.Cleanup(func() {
		_ = ps.Close()
	})

	receivedCh := make(chan *Message, 1)
	sub, err := ps.Subscribe(ctx, &SubscribeRequest{
		Topic:         topic,
		ConsumerGroup: group,
		Handler: func(ctx context.Context, msg *Message) error {
			receivedCh <- msg
			return nil
		},
	})
	if err != nil {
		t.Fatalf("subscribe failed: %v", err)
	}
	t.Cleanup(func() {
		_ = sub.Close()
	})

	err = ps.Publish(ctx, &PublishRequest{
		Topic:       topic,
		Data:        []byte("hello"),
		ContentType: "text/plain",
		Metadata: map[string]string{
			"k": "v",
		},
	})
	if err != nil {
		t.Fatalf("publish failed: %v", err)
	}

	msg := waitMessage(t, receivedCh)
	if msg.Topic != topic {
		t.Fatalf("unexpected topic: %s", msg.Topic)
	}
	if string(msg.Data) != "hello" {
		t.Fatalf("unexpected data: %s", string(msg.Data))
	}
	if msg.ContentType != "text/plain" {
		t.Fatalf("unexpected content type: %s", msg.ContentType)
	}
	if msg.Metadata["k"] != "v" {
		t.Fatalf("unexpected metadata: %+v", msg.Metadata)
	}

	waitForCondition(t, time.Second, func() bool {
		pending, err := rdb.XPending(ctx, topic, group).Result()
		return err == nil && pending.Count == 0
	})
}

func TestRedisPubSubCreatesConsumerGroup(t *testing.T) {
	ctx := context.Background()
	rdb := newTestRedisClient(t)

	topic := testStreamName("group-create")
	group := testStreamName("group")

	ps := NewRedisPubSub(rdb, WithBlockTimeout(50*time.Millisecond))
	t.Cleanup(func() {
		_ = ps.Close()
	})

	sub, err := ps.Subscribe(ctx, &SubscribeRequest{
		Topic:         topic,
		ConsumerGroup: group,
		Handler: func(ctx context.Context, msg *Message) error {
			return nil
		},
	})
	if err != nil {
		t.Fatalf("subscribe failed: %v", err)
	}
	t.Cleanup(func() {
		_ = sub.Close()
	})

	groups, err := rdb.XInfoGroups(ctx, topic).Result()
	if err != nil {
		t.Fatalf("xinfo groups failed: %v", err)
	}
	if len(groups) != 1 || groups[0].Name != group {
		t.Fatalf("unexpected groups: %+v", groups)
	}
}

func TestRedisPubSubRedeliverPending(t *testing.T) {
	ctx := context.Background()
	rdb := newTestRedisClient(t)

	topic := testStreamName("redeliver")
	group := testStreamName("group")
	handlerErr := errors.New("handler failed")

	ps := NewRedisPubSub(rdb,
		WithBlockTimeout(20*time.Millisecond),
		WithReadCount(4),
		WithConcurrency(2),
		WithProcessingTimeout(100*time.Millisecond),
		WithRedeliverInterval(50*time.Millisecond),
	)
	t.Cleanup(func() {
		_ = ps.Close()
	})

	firstCalled := make(chan struct{}, 1)
	firstSub, err := ps.Subscribe(ctx, &SubscribeRequest{
		Topic:         topic,
		ConsumerGroup: group,
		Handler: func(ctx context.Context, msg *Message) error {
			firstCalled <- struct{}{}
			return handlerErr
		},
	})
	if err != nil {
		t.Fatalf("first subscribe failed: %v", err)
	}

	err = ps.Publish(ctx, &PublishRequest{
		Topic: topic,
		Data:  []byte("retry-me"),
	})
	if err != nil {
		t.Fatalf("publish failed: %v", err)
	}

	select {
	case <-firstCalled:
	case <-time.After(2 * time.Second):
		t.Fatal("first handler was not called")
	}

	if err := firstSub.Wait(); !errors.Is(err, handlerErr) {
		t.Fatalf("unexpected first wait err: %v", err)
	}

	waitForCondition(t, time.Second, func() bool {
		pending, err := rdb.XPending(ctx, topic, group).Result()
		return err == nil && pending.Count == 1
	})

	redeliveredCh := make(chan *Message, 1)
	secondSub, err := ps.Subscribe(ctx, &SubscribeRequest{
		Topic:         topic,
		ConsumerGroup: group,
		Handler: func(ctx context.Context, msg *Message) error {
			redeliveredCh <- msg
			return nil
		},
	})
	if err != nil {
		t.Fatalf("second subscribe failed: %v", err)
	}
	t.Cleanup(func() {
		_ = secondSub.Close()
	})

	msg := waitMessage(t, redeliveredCh)
	if string(msg.Data) != "retry-me" {
		t.Fatalf("unexpected redelivered data: %s", string(msg.Data))
	}

	waitForCondition(t, 2*time.Second, func() bool {
		pending, err := rdb.XPending(ctx, topic, group).Result()
		return err == nil && pending.Count == 0
	})
}

func TestRedisPubSubCloseReturnsNilWait(t *testing.T) {
	ctx := context.Background()
	rdb := newTestRedisClient(t)

	ps := NewRedisPubSub(rdb, WithBlockTimeout(20*time.Millisecond))
	t.Cleanup(func() {
		_ = ps.Close()
	})

	sub, err := ps.Subscribe(ctx, &SubscribeRequest{
		Topic:         testStreamName("close"),
		ConsumerGroup: testStreamName("group"),
		Handler: func(ctx context.Context, msg *Message) error {
			return nil
		},
	})
	if err != nil {
		t.Fatalf("subscribe failed: %v", err)
	}

	if err := sub.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}
	if err := sub.Wait(); err != nil {
		t.Fatalf("wait should be nil after close, got: %v", err)
	}
}

func TestRedisPubSubGracefulCloseDrainsInflight(t *testing.T) {
	ctx := context.Background()
	rdb := newTestRedisClient(t)

	topic := testStreamName("graceful-close")
	group := testStreamName("group")

	ps := NewRedisPubSub(rdb,
		WithBlockTimeout(20*time.Millisecond),
		WithReadCount(4),
		WithConcurrency(1),
		WithProcessingTimeout(200*time.Millisecond),
		WithRedeliverInterval(50*time.Millisecond),
	)
	t.Cleanup(func() {
		_ = ps.Close()
	})

	started := make(chan struct{}, 1)
	release := make(chan struct{})

	sub, err := ps.Subscribe(ctx, &SubscribeRequest{
		Topic:         topic,
		ConsumerGroup: group,
		Handler: func(ctx context.Context, msg *Message) error {
			started <- struct{}{}
			<-release
			return nil
		},
	})
	if err != nil {
		t.Fatalf("subscribe failed: %v", err)
	}

	if err := ps.Publish(ctx, &PublishRequest{
		Topic: topic,
		Data:  []byte("drain-me"),
	}); err != nil {
		t.Fatalf("publish failed: %v", err)
	}

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("handler did not start")
	}

	if err := sub.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}
	close(release)

	if err := sub.Wait(); err != nil {
		t.Fatalf("wait should be nil after graceful close, got: %v", err)
	}

	waitForCondition(t, time.Second, func() bool {
		pending, err := rdb.XPending(ctx, topic, group).Result()
		return err == nil && pending.Count == 0
	})
}

func TestRedisPubSubValidation(t *testing.T) {
	ps := NewRedisPubSub(nil)

	if err := ps.Publish(context.Background(), nil); !errors.Is(err, ErrNilRedisClient) {
		t.Fatalf("unexpected nil client publish err: %v", err)
	}

	rdb := redis.NewClient(&redis.Options{
		Addr:         "127.0.0.1:6379",
		Password:     "redis-test-0",
		DB:           15,
		MaxRetries:   0,
		DialTimeout:  300 * time.Millisecond,
		ReadTimeout:  time.Second,
		WriteTimeout: time.Second,
	})
	defer rdb.Close()

	ps = NewRedisPubSub(rdb)

	if err := ps.Publish(context.Background(), nil); !errors.Is(err, ErrNilPublishRequest) {
		t.Fatalf("unexpected nil publish request err: %v", err)
	}

	if _, err := ps.Subscribe(context.Background(), nil); !errors.Is(err, ErrNilSubscribeRequest) {
		t.Fatalf("unexpected nil subscribe request err: %v", err)
	}

	if err := ps.Publish(context.Background(), &PublishRequest{}); !errors.Is(err, ErrEmptyTopic) {
		t.Fatalf("unexpected empty topic err: %v", err)
	}

	if _, err := ps.Subscribe(context.Background(), &SubscribeRequest{Topic: "a"}); !errors.Is(err, ErrEmptyConsumerGroup) {
		t.Fatalf("unexpected empty group err: %v", err)
	}

	if _, err := ps.Subscribe(context.Background(), &SubscribeRequest{
		Topic:         "a",
		ConsumerGroup: "g",
	}); !errors.Is(err, ErrNilHandler) {
		t.Fatalf("unexpected nil handler err: %v", err)
	}
}

func newTestRedisClient(t *testing.T) *redis.Client {
	t.Helper()

	rdb := redis.NewClient(&redis.Options{
		Addr:         "127.0.0.1:6379",
		Password:     "redis-test-0",
		DB:           15,
		MaxRetries:   0,
		DialTimeout:  300 * time.Millisecond,
		ReadTimeout:  time.Second,
		WriteTimeout: time.Second,
	})

	ctx := context.Background()
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Skipf("redis is unavailable: %v", err)
	}

	if err := rdb.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flush db failed: %v", err)
	}

	t.Cleanup(func() {
		_ = rdb.FlushDB(ctx).Err()
		_ = rdb.Close()
	})

	return rdb
}

func waitMessage(t *testing.T, ch <-chan *Message) *Message {
	t.Helper()

	select {
	case msg := <-ch:
		return msg
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for message")
		return nil
	}
}

func waitForCondition(t *testing.T, timeout time.Duration, fn func() bool) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}

	t.Fatal("condition was not satisfied before timeout")
}

func testStreamName(prefix string) string {
	return prefix + ":" + uuid.NewString()
}
