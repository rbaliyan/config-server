//go:build integration

package peersync

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/rbaliyan/config-server/internal/testutil"
)

// TestRedisTransport_Integration_PublishSubscribe exercises the real Redis
// pub/sub Transport against a live server. It skips unless REDIS_ADDR is set,
// mirroring the env-gated style used by the MongoDB/PostgreSQL integration
// suites. Run with:
//
//	REDIS_ADDR=localhost:6379 go test -tags=integration -run TestRedis ./peersync/
func TestRedisTransport_Integration_PublishSubscribe(t *testing.T) {
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		t.Skip("REDIS_ADDR not set")
	}

	client := goredis.NewClient(&goredis.Options{Addr: addr})
	t.Cleanup(func() { _ = client.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Fail fast if the server is unreachable so the test reports a clear error
	// instead of hanging on the first pub/sub call.
	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatalf("redis ping %s: %v", addr, err)
	}

	tr, err := NewRedisTransport(client, "config:sync:integration-test")
	if err != nil {
		t.Fatalf("NewRedisTransport: %v", err)
	}
	t.Cleanup(func() { _ = tr.Close() })

	var (
		mu       sync.Mutex
		received [][]byte
		done     = make(chan struct{})
		once     sync.Once
	)
	want := []byte("integration-payload")

	if err := tr.Subscribe(ctx, func(msg []byte) {
		mu.Lock()
		received = append(received, msg)
		mu.Unlock()
		once.Do(func() { close(done) })
	}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	// Real Redis registers the subscriber asynchronously and drops messages
	// published before it is live; re-publish on an interval until delivery is
	// observed (or the bounded poll fails).
	delivered := testutil.Eventually(5*time.Second, 50*time.Millisecond, func() bool {
		if err := tr.Publish(ctx, want); err != nil {
			t.Errorf("Publish: %v", err)
			return true
		}
		select {
		case <-done:
			return true
		default:
			return false
		}
	})
	if !delivered {
		t.Fatal("timeout waiting for message over real Redis")
	}

	mu.Lock()
	got := received[0]
	mu.Unlock()
	if string(got) != string(want) {
		t.Errorf("received %q, want %q", got, want)
	}

	// Sanity: the live transport reports healthy.
	if err := tr.(TransportHealthChecker).Health(ctx); err != nil {
		t.Errorf("Health on live Redis: %v", err)
	}
}

// TestRedisTransport_Integration_HealthAfterBrokerLoss verifies the failure
// path: once the underlying connection is gone, Health must report unhealthy
// rather than reporting healthy or hanging. This exercises broker-down
// detection against a real server.
func TestRedisTransport_Integration_HealthAfterBrokerLoss(t *testing.T) {
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		t.Skip("REDIS_ADDR not set")
	}

	client := goredis.NewClient(&goredis.Options{Addr: addr})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatalf("redis ping %s: %v", addr, err)
	}

	tr, err := NewRedisTransport(client, "config:sync:integration-health")
	if err != nil {
		t.Fatalf("NewRedisTransport: %v", err)
	}

	// Live broker: healthy.
	if err := tr.(TransportHealthChecker).Health(ctx); err != nil {
		t.Fatalf("Health on live Redis: %v", err)
	}

	// Simulate broker loss by closing the client out from under the transport.
	if err := client.Close(); err != nil {
		t.Fatalf("close client: %v", err)
	}

	// Health must now surface the failure within a bounded time (no hang).
	hctx, hcancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer hcancel()
	if err := tr.(TransportHealthChecker).Health(hctx); err == nil {
		t.Error("Health reported healthy after the broker connection was closed")
	}
}
