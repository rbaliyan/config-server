package peersync

import (
	"context"
	"sync"

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
	wg      sync.WaitGroup
	once    sync.Once
}

func (t *redisTransport) Publish(ctx context.Context, payload []byte) error {
	return t.client.Publish(ctx, t.channel, payload).Err()
}

func (t *redisTransport) Subscribe(ctx context.Context, handler func([]byte)) error {
	t.once.Do(func() {
		sub := t.client.Subscribe(ctx, t.channel)
		bctx, cancel := context.WithCancel(context.Background())
		t.cancel = cancel
		t.wg.Add(1)
		go func() {
			defer t.wg.Done()
			defer sub.Close() //nolint:errcheck // Redis pub/sub Close is best-effort on shutdown
			ch := sub.Channel()
			for {
				select {
				case <-bctx.Done():
					return
				case raw, ok := <-ch:
					if !ok {
						return
					}
					handler([]byte(raw.Payload))
				}
			}
		}()
	})
	return nil
}

func (t *redisTransport) Close() error {
	if t.cancel != nil {
		t.cancel()
	}
	t.wg.Wait()
	return nil
}
