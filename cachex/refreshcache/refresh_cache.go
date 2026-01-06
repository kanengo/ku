package refreshcache

import (
	"errors"
	"log/slog"
	"sync/atomic"
	"time"
)

type RefreshCache[T any] struct {
	ttl     time.Duration
	fetcher func() (T, error)
	v       atomic.Value
}

func NewRefreshCache[T any](ttl time.Duration, fetcher func() (T, error)) (*RefreshCache[T], error) {
	if ttl <= 0 {
		return nil, errors.New("ttl must be greater than 0")
	}
	if fetcher == nil {
		return nil, errors.New("fetcher must not be nil")
	}
	c := &RefreshCache[T]{
		ttl:     ttl,
		fetcher: fetcher,
	}

	if err := c.Init(); err != nil {
		return nil, err
	}
	return c, nil
}

func (c *RefreshCache[T]) Init() error {
	if err := c.refresh(); err != nil {
		return err
	}
	go func() {
		ticker := time.NewTicker(c.ttl)
		defer ticker.Stop()
		for range ticker.C {
			if err := c.refresh(); err != nil {
				slog.Error("refresh cache failed", "err", err)
				continue
			}
		}
	}()
	return nil
}

func (c *RefreshCache[T]) Get() (T, error) {
	return c.v.Load().(T), nil
}

func (c *RefreshCache[T]) refresh() error {
	v, err := c.fetcher()
	if err != nil {
		return err
	}
	c.v.Store(v)
	return nil
}
