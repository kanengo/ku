package mqx

import "context"

type Message struct {
	Topic       string
	Data        []byte
	Metadata    map[string]string
	ContentType string
}

type MessageHandler func(context.Context, *Message) error

type PublishRequest struct {
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
