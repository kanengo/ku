package mqx

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bytedance/sonic"
	"github.com/kanengo/ku/unsafex"
	"github.com/redis/go-redis/v9"
)

const redisStreamPayloadField = "payload"

var (
	ErrNilRedisClient      = errors.New("mqx: redis client is nil")
	ErrNilPublishRequest   = errors.New("mqx: publish request is nil")
	ErrNilSubscribeRequest = errors.New("mqx: subscribe request is nil")
	ErrEmptyTopic          = errors.New("mqx: topic is empty")
	ErrEmptyConsumerGroup  = errors.New("mqx: consumer group is empty")
	ErrNilHandler          = errors.New("mqx: handler is nil")
	ErrPubSubClosed        = errors.New("mqx: pubsub is closed")
	ErrInvalidPayload      = errors.New("mqx: invalid redis stream payload")
)

type redisEnvelope struct {
	ID          string
	Data        []byte            `json:"data"`
	Metadata    map[string]string `json:"metadata,omitempty"`
	ContentType string            `json:"content_type,omitempty"`
	SentAt      int64             `json:"sent_at,omitempty"`
}

type redisPubSubOptions struct {
	blockTimeout      time.Duration
	readCount         int64
	concurrency       int
	processingTimeout time.Duration
	redeliverInterval time.Duration
	maxLenApprox      int64
}

var defaultRedisPubSubOptions = redisPubSubOptions{
	blockTimeout:      time.Second * 2,
	readCount:         int64(runtime.NumCPU() * 2),
	concurrency:       runtime.NumCPU(),
	processingTimeout: 30 * time.Second,
	redeliverInterval: 5 * time.Second,
	maxLenApprox:      10000,
}

type RedisPubSubOption func(*redisPubSubOptions)

func WithBlockTimeout(timeout time.Duration) RedisPubSubOption {
	return func(o *redisPubSubOptions) {
		if timeout > 0 {
			o.blockTimeout = timeout
		}
	}
}

func WithReadCount(count int64) RedisPubSubOption {
	return func(o *redisPubSubOptions) {
		if count > 0 {
			o.readCount = count
		}
	}
}

func WithConcurrency(num int) RedisPubSubOption {
	return func(o *redisPubSubOptions) {
		if num > 0 {
			o.concurrency = num
		}
	}
}

func WithProcessingTimeout(timeout time.Duration) RedisPubSubOption {
	return func(o *redisPubSubOptions) {
		if timeout > 0 {
			o.processingTimeout = timeout
		}
	}
}

func WithRedeliverInterval(interval time.Duration) RedisPubSubOption {
	return func(o *redisPubSubOptions) {
		if interval > 0 {
			o.redeliverInterval = interval
		}
	}
}

func WithMaxLenApprox(maxLen int64) RedisPubSubOption {
	return func(o *redisPubSubOptions) {
		if maxLen > 0 {
			o.maxLenApprox = maxLen
		}
	}
}

type RedisPubSub struct {
	client redis.UniversalClient
	opts   redisPubSubOptions

	mu     sync.Mutex
	closed bool
	subs   map[*redisSubscription]struct{}
}

func NewRedisPubSub(client redis.UniversalClient, opts ...RedisPubSubOption) *RedisPubSub {
	options := defaultRedisPubSubOptions
	for _, opt := range opts {
		opt(&options)
	}

	return &RedisPubSub{
		client: client,
		opts:   options,
		subs:   make(map[*redisSubscription]struct{}),
	}
}

func (p *RedisPubSub) Publish(ctx context.Context, req *PublishRequest) error {
	if p.client == nil {
		return ErrNilRedisClient
	}
	if req == nil {
		return ErrNilPublishRequest
	}
	if req.Topic == "" {
		return ErrEmptyTopic
	}

	payload, err := sonic.MarshalString(redisEnvelope{
		ID:          req.ID,
		Data:        req.Data,
		Metadata:    req.Metadata,
		ContentType: req.ContentType,
		SentAt:      time.Now().UnixMilli(),
	})
	if err != nil {
		return err
	}

	args := &redis.XAddArgs{
		Stream: req.Topic,
		Values: map[string]any{
			redisStreamPayloadField: payload,
		},
	}
	if p.opts.maxLenApprox > 0 {
		args.MaxLen = p.opts.maxLenApprox
		args.Approx = true
	}

	return p.client.XAdd(ctx, args).Err()
}

func (p *RedisPubSub) Subscribe(ctx context.Context, req *SubscribeRequest) (Subscription, error) {
	if p.client == nil {
		return nil, ErrNilRedisClient
	}
	if req == nil {
		return nil, ErrNilSubscribeRequest
	}
	if req.Topic == "" {
		return nil, ErrEmptyTopic
	}
	if req.ConsumerGroup == "" {
		return nil, ErrEmptyConsumerGroup
	}
	if req.Handler == nil {
		return nil, ErrNilHandler
	}

	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, ErrPubSubClosed
	}
	p.mu.Unlock()

	if err := p.ensureGroup(ctx, req.Topic, req.ConsumerGroup); err != nil {
		return nil, err
	}

	intakeCtx, cancelIntake := context.WithCancel(ctx)
	processCtx, cancelProcess := context.WithCancel(ctx)
	sub := &redisSubscription{
		parent:        p,
		client:        p.client,
		opts:          p.opts,
		topic:         req.Topic,
		consumerGroup: req.ConsumerGroup,
		consumerName:  req.ConsumerGroup,
		handler:       req.Handler,
		intakeCtx:     intakeCtx,
		cancelIntake:  cancelIntake,
		processCtx:    processCtx,
		cancelProcess: cancelProcess,
		doneCh:        make(chan struct{}),
		inflight:      make(map[string]struct{}),
	}

	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		cancelIntake()
		cancelProcess()
		return nil, ErrPubSubClosed
	}
	p.subs[sub] = struct{}{}
	p.mu.Unlock()

	go sub.run()

	return sub, nil
}

func (p *RedisPubSub) Close() error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	subs := make([]*redisSubscription, 0, len(p.subs))
	for sub := range p.subs {
		subs = append(subs, sub)
	}
	p.mu.Unlock()

	for _, sub := range subs {
		_ = sub.Close()
	}

	return nil
}

func (p *RedisPubSub) ensureGroup(ctx context.Context, stream, group string) error {
	err := p.client.XGroupCreateMkStream(ctx, stream, group, "$").Err()
	if err == nil {
		return nil
	}
	if strings.Contains(err.Error(), "BUSYGROUP") {
		return nil
	}
	return err
}

func (p *RedisPubSub) removeSubscription(sub *redisSubscription) {
	p.mu.Lock()
	delete(p.subs, sub)
	p.mu.Unlock()
}

type redisSubscription struct {
	parent *RedisPubSub
	client redis.UniversalClient
	opts   redisPubSubOptions

	topic         string
	consumerGroup string
	consumerName  string
	handler       MessageHandler

	intakeCtx     context.Context
	cancelIntake  context.CancelFunc
	processCtx    context.Context
	cancelProcess context.CancelFunc

	closeOnce    sync.Once
	shutdownOnce sync.Once
	stopOnce     sync.Once
	doneCh       chan struct{}

	closed atomic.Bool

	waitMu     sync.Mutex
	waitErr    error
	inflightMu sync.Mutex
	inflight   map[string]struct{}
}

func (s *redisSubscription) Close() error {
	s.closeOnce.Do(func() {
		s.closed.Store(true)
		s.initiateShutdown()
	})
	return nil
}

func (s *redisSubscription) Wait() error {
	<-s.doneCh
	s.waitMu.Lock()
	defer s.waitMu.Unlock()
	return s.waitErr
}

func (s *redisSubscription) run() {
	defer close(s.doneCh)
	defer s.parent.removeSubscription(s)

	msgCh := make(chan redis.XMessage, s.messageBufferSize())

	var producerWG sync.WaitGroup
	producerWG.Add(2)
	go s.readLoop(msgCh, &producerWG)
	go s.reclaimLoop(msgCh, &producerWG)

	var workerWG sync.WaitGroup
	for range s.opts.concurrency {
		workerWG.Add(1)
		go s.workerLoop(msgCh, &workerWG)
	}

	producerWG.Wait()
	close(msgCh)
	workerWG.Wait()
	s.cancelProcess()
	s.stopOnce.Do(func() {
		s.finish(nil)
	})
}

func (s *redisSubscription) reclaimPending(ctx context.Context) ([]redis.XMessage, error) {
	start := "0-0"
	var claimed []redis.XMessage

	for {
		msgs, next, err := s.client.XAutoClaim(ctx, &redis.XAutoClaimArgs{
			Stream:   s.topic,
			Group:    s.consumerGroup,
			MinIdle:  s.opts.processingTimeout,
			Start:    start,
			Count:    s.opts.readCount,
			Consumer: s.consumerName,
		}).Result()
		if err != nil {
			if errors.Is(err, redis.Nil) {
				return claimed, nil
			}
			return nil, err
		}
		if len(msgs) == 0 {
			return claimed, nil
		}
		claimed = append(claimed, msgs...)
		if next == "" || next == "0-0" || next == start {
			return claimed, nil
		}
		start = next
	}
}

func (s *redisSubscription) handleMessage(ctx context.Context, raw redis.XMessage) error {
	defer s.untrackMessage(raw.ID)

	msg, err := decodeRedisMessage(s.topic, raw)
	if err != nil {
		// Failed to decode message, ack it to move forward
		ackCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return s.client.XAck(ackCtx, s.topic, s.consumerGroup, raw.ID).Err()
	}

	if err := s.handler(ctx, msg); err != nil {
		return err
	}

	ackCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	return s.client.XAck(ackCtx, s.topic, s.consumerGroup, raw.ID).Err()
}

func (s *redisSubscription) readLoop(msgCh chan<- redis.XMessage, wg *sync.WaitGroup) {
	defer wg.Done()

	for {
		if err := s.intakeCtx.Err(); err != nil {
			return
		}

		streams, err := s.client.XReadGroup(s.intakeCtx, &redis.XReadGroupArgs{
			Group:    s.consumerGroup,
			Consumer: s.consumerName,
			Streams:  []string{s.topic, ">"},
			Count:    s.opts.readCount,
			Block:    s.opts.blockTimeout,
		}).Result()
		if err != nil {
			if s.intakeCtx.Err() != nil || errors.Is(err, context.Canceled) {
				return
			}
			if errors.Is(err, redis.Nil) {
				continue
			}
			s.fatalStop(err)
			return
		}

		for _, stream := range streams {
			for _, msg := range stream.Messages {
				if !s.enqueueMessage(msgCh, msg) {
					return
				}
			}
		}
	}
}

func (s *redisSubscription) reclaimLoop(msgCh chan<- redis.XMessage, wg *sync.WaitGroup) {
	defer wg.Done()

	ticker := time.NewTicker(s.opts.redeliverInterval)
	defer ticker.Stop()

	if !s.reclaimOnce(msgCh) {
		return
	}

	for {
		select {
		case <-s.intakeCtx.Done():
			return
		case <-ticker.C:
			if !s.reclaimOnce(msgCh) {
				return
			}
		}
	}
}

func (s *redisSubscription) reclaimOnce(msgCh chan<- redis.XMessage) bool {
	msgs, err := s.reclaimPending(s.intakeCtx)
	if err != nil {
		if s.intakeCtx.Err() != nil || errors.Is(err, context.Canceled) {
			return false
		}
		s.fatalStop(err)
		return false
	}

	for _, raw := range msgs {
		if !s.enqueueMessage(msgCh, raw) {
			return false
		}
	}
	return true
}

func (s *redisSubscription) workerLoop(msgCh <-chan redis.XMessage, wg *sync.WaitGroup) {
	defer wg.Done()

	for raw := range msgCh {
		if s.processCtx.Err() != nil {
			return
		}
		if err := s.handleMessage(s.processCtx, raw); err != nil {
			// s.fatalStop(err)
			slog.Error("Failed to handle message", "err", err, "message", raw)
		}
	}
}

func (s *redisSubscription) finish(err error) {
	s.waitMu.Lock()
	defer s.waitMu.Unlock()
	s.waitErr = err
}

func (s *redisSubscription) fatalStop(err error) {
	s.stopOnce.Do(func() {
		s.finish(s.normalizeRuntimeErr(err))
		s.initiateShutdown()
		s.cancelProcess()
	})
}

func (s *redisSubscription) enqueueMessage(msgCh chan<- redis.XMessage, msg redis.XMessage) bool {
	if !s.trackMessage(msg.ID) {
		return true
	}

	select {
	case <-s.intakeCtx.Done():
		s.untrackMessage(msg.ID)
		return false
	case msgCh <- msg:
		return true
	}
}

func (s *redisSubscription) messageBufferSize() int {
	size := int(s.opts.readCount)
	if size <= 0 {
		size = 1
	}
	if s.opts.concurrency > 1 {
		size *= s.opts.concurrency
	}
	if size < s.opts.concurrency {
		size = s.opts.concurrency
	}
	return size
}

func (s *redisSubscription) initiateShutdown() {
	s.shutdownOnce.Do(func() {
		s.cancelIntake()
	})
}

func (s *redisSubscription) trackMessage(id string) bool {
	s.inflightMu.Lock()
	defer s.inflightMu.Unlock()

	if _, ok := s.inflight[id]; ok {
		return false
	}
	s.inflight[id] = struct{}{}
	return true
}

func (s *redisSubscription) untrackMessage(id string) {
	s.inflightMu.Lock()
	delete(s.inflight, id)
	s.inflightMu.Unlock()
}

func (s *redisSubscription) normalizeRuntimeErr(err error) error {
	if err == nil {
		return nil
	}
	if s.closed.Load() || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return nil
	}
	if errors.Is(s.intakeCtx.Err(), context.Canceled) && s.closed.Load() {
		return nil
	}
	return err
}

func decodeRedisMessage(topic string, msg redis.XMessage) (*Message, error) {
	rawPayload, ok := msg.Values[redisStreamPayloadField]
	if !ok {
		return nil, fmt.Errorf("%w: missing payload field", ErrInvalidPayload)
	}

	payload, err := payloadString(rawPayload)
	if err != nil {
		return nil, err
	}

	var envelope redisEnvelope
	if err := sonic.UnmarshalString(payload, &envelope); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidPayload, err)
	}

	id := msg.ID
	if envelope.ID != "" {
		id = envelope.ID
	}

	return &Message{
		ID:          id,
		Topic:       topic,
		Data:        envelope.Data,
		Metadata:    envelope.Metadata,
		ContentType: envelope.ContentType,
		SentAt:      envelope.SentAt,
	}, nil
}

func payloadString(v any) (string, error) {
	switch payload := v.(type) {
	case string:
		return payload, nil
	case []byte:
		return unsafex.Bytes2String(payload), nil
	default:
		return "", fmt.Errorf("%w: unexpected payload type %T", ErrInvalidPayload, v)
	}
}
