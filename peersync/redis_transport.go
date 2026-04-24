package peersync

import (
	"context"
	"encoding/json"

	goredis "github.com/redis/go-redis/v9"
)

const defaultChannel = "config:sync"

// NewRedisTransport creates a Transport backed by Redis pub/sub.
// All cluster nodes must connect to the same Redis instance or cluster.
// If channel is empty, "config:sync" is used.
func NewRedisTransport(client goredis.UniversalClient, channel string) Transport {
	if channel == "" {
		channel = defaultChannel
	}
	return &redisTransport{client: client, channel: channel}
}

type redisTransport struct {
	client  goredis.UniversalClient
	channel string
	cancel  context.CancelFunc
}

func (t *redisTransport) Publish(ctx context.Context, msg Message) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	return t.client.Publish(ctx, t.channel, data).Err()
}

func (t *redisTransport) Subscribe(ctx context.Context, handler func(Message)) error {
	sub := t.client.Subscribe(ctx, t.channel)
	bctx, cancel := context.WithCancel(context.Background())
	t.cancel = cancel
	go func() {
		defer sub.Close()
		ch := sub.Channel()
		for {
			select {
			case <-bctx.Done():
				return
			case raw, ok := <-ch:
				if !ok {
					return
				}
				var msg Message
				if err := json.Unmarshal([]byte(raw.Payload), &msg); err != nil {
					continue
				}
				handler(msg)
			}
		}
	}()
	return nil
}

func (t *redisTransport) Close() error {
	if t.cancel != nil {
		t.cancel()
	}
	return nil
}
