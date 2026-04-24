package peersync

import (
	"context"
	"errors"
	"sync"

	goredis "github.com/redis/go-redis/v9"
)

// defaultChannel is the Redis pub/sub channel used when none is specified.
// The name is arbitrary; any non-empty string unique to this cluster works.
const defaultChannel = "config:sync"

// NewRedisTransport creates a Transport backed by Redis pub/sub. All cluster
// nodes must connect to the same Redis instance or cluster and the same
// channel. If channel is empty, "config:sync" is used. client must not be nil.
//
// Subscribe starts the background reader. A second call returns an error;
// since SyncStore calls Subscribe exactly once during Connect, this is never
// reached in normal use.
//
// Publish after Close returns the underlying Redis client error.
func NewRedisTransport(client goredis.UniversalClient, channel string) (Transport, error) {
	if client == nil {
		return nil, errors.New("peersync: redis client must not be nil")
	}
	if channel == "" {
		channel = defaultChannel
	}
	return &redisTransport{client: client, channel: channel}, nil
}

var _ Transport = (*redisTransport)(nil)
var _ TransportHealthChecker = (*redisTransport)(nil)

type redisTransport struct {
	client     goredis.UniversalClient
	channel    string
	cancel     context.CancelFunc
	wg         sync.WaitGroup
	subMu      sync.Mutex
	subscribed bool
}

func (t *redisTransport) Publish(ctx context.Context, payload []byte) error {
	return t.client.Publish(ctx, t.channel, payload).Err()
}

func (t *redisTransport) Subscribe(ctx context.Context, handler func([]byte)) error {
	t.subMu.Lock()
	defer t.subMu.Unlock()
	if t.subscribed {
		return errors.New("peersync: transport already has an active subscription")
	}
	t.subscribed = true
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
				safeCall(handler, []byte(raw.Payload))
			}
		}
	}()
	return nil
}

func (t *redisTransport) Close() error {
	if t.cancel != nil {
		t.cancel()
	}
	t.wg.Wait()
	return nil
}

func (t *redisTransport) Health(ctx context.Context) error {
	return t.client.Ping(ctx).Err()
}
