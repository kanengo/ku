package mqx

import "context"

type Message struct {
	ID          string
	Topic       string
	Data        []byte
	Metadata    map[string]string
	ContentType string
	SentAt      int64 // ms
}

type MessageHandler func(context.Context, *Message) error

type PublishRequest struct {
	ID          string
	Topic       string
	Data        []byte
	Metadata    map[string]string
	ContentType string
}

type SubscribeRequest struct {
	Topic         string
	ConsumerGroup string
	Handler       MessageHandler
}

type Subscription interface {
	Close() error
	Wait() error
}

type PubSub interface {
	Publish(context.Context, *PublishRequest) error
	Subscribe(context.Context, *SubscribeRequest) (Subscription, error)
	Close() error
}
