package peersync

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"
)

func newTestRedisClient(t *testing.T) (*miniredis.Miniredis, goredis.UniversalClient) {
	t.Helper()
	mr := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	return mr, client
}

func TestNewRedisTransport_NilClient(t *testing.T) {
	_, err := NewRedisTransport(nil, "")
	if err == nil {
		t.Fatal("expected error for nil client, got nil")
	}
}

func TestNewRedisTransport_DefaultChannel(t *testing.T) {
	_, client := newTestRedisClient(t)
	tr, err := NewRedisTransport(client, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	rt := tr.(*redisTransport)
	if rt.channel != defaultChannel {
		t.Errorf("channel = %q, want %q", rt.channel, defaultChannel)
	}
}

func TestRedisTransport_PublishSubscribe(t *testing.T) {
	_, client := newTestRedisClient(t)
	tr, err := NewRedisTransport(client, "test:chan")
	if err != nil {
		t.Fatalf("NewRedisTransport: %v", err)
	}
	defer tr.Close()

	ctx := context.Background()
	var (
		mu      sync.Mutex
		received [][]byte
		done    = make(chan struct{})
	)
	want := []byte("hello")

	if err := tr.Subscribe(ctx, func(msg []byte) {
		mu.Lock()
		received = append(received, msg)
		mu.Unlock()
		close(done)
	}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	// Brief pause to let the subscription goroutine register with Redis.
	time.Sleep(50 * time.Millisecond)

	if err := tr.Publish(ctx, want); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for published message")
	}

	mu.Lock()
	got := received[0]
	mu.Unlock()
	if string(got) != string(want) {
		t.Errorf("received %q, want %q", got, want)
	}
}

func TestRedisTransport_SubscribeSecondCallErrors(t *testing.T) {
	_, client := newTestRedisClient(t)
	tr, err := NewRedisTransport(client, "test:idem")
	if err != nil {
		t.Fatalf("NewRedisTransport: %v", err)
	}
	defer tr.Close()

	ctx := context.Background()

	if err := tr.Subscribe(ctx, func([]byte) {}); err != nil {
		t.Fatalf("first Subscribe: %v", err)
	}

	// Second Subscribe must return an error.
	if err := tr.Subscribe(ctx, func([]byte) {}); err == nil {
		t.Error("second Subscribe: expected error, got nil")
	}
}

func TestRedisTransport_CloseStopsReader(t *testing.T) {
	_, client := newTestRedisClient(t)
	tr, err := NewRedisTransport(client, "test:close")
	if err != nil {
		t.Fatalf("NewRedisTransport: %v", err)
	}

	ctx := context.Background()
	if err := tr.Subscribe(ctx, func([]byte) {}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	done := make(chan error, 1)
	go func() {
		done <- tr.Close()
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Close returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not return in time")
	}
}

func TestRedisTransport_CloseWithoutSubscribe(t *testing.T) {
	_, client := newTestRedisClient(t)
	tr, err := NewRedisTransport(client, "test:nosub")
	if err != nil {
		t.Fatalf("NewRedisTransport: %v", err)
	}
	// Close before Subscribe must not panic or block.
	if err := tr.Close(); err != nil {
		t.Errorf("Close without Subscribe: %v", err)
	}
}

func TestRedisTransport_Health(t *testing.T) {
	_, client := newTestRedisClient(t)
	tr, err := NewRedisTransport(client, "")
	if err != nil {
		t.Fatalf("NewRedisTransport: %v", err)
	}
	defer tr.Close()

	hc, ok := tr.(TransportHealthChecker)
	if !ok {
		t.Fatal("redisTransport does not implement TransportHealthChecker")
	}
	if err := hc.Health(context.Background()); err != nil {
		t.Errorf("Health on live server: %v", err)
	}
}

func TestRedisTransport_HealthDown(t *testing.T) {
	mr, client := newTestRedisClient(t)
	tr, err := NewRedisTransport(client, "")
	if err != nil {
		t.Fatalf("NewRedisTransport: %v", err)
	}
	defer tr.Close()

	mr.Close() // shut down the Redis server

	hc := tr.(TransportHealthChecker)
	if err := hc.Health(context.Background()); err == nil {
		t.Error("expected Health error after server shutdown, got nil")
	}
}

func TestRedisTransport_PublishAfterClose(t *testing.T) {
	_, client := newTestRedisClient(t)
	tr, err := NewRedisTransport(client, "test:pubclose")
	if err != nil {
		t.Fatalf("NewRedisTransport: %v", err)
	}
	ctx := context.Background()
	if err := tr.Subscribe(ctx, func([]byte) {}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	_ = tr.Close()

	// Publish after Close should return an error from the Redis client.
	err = tr.Publish(ctx, []byte("late"))
	if err == nil {
		t.Log("note: Publish after Close returned nil (client may buffer)")
	}
	// We don't assert a specific error here since go-redis may or may not
	// return an error after the connection is closed; the important thing is
	// it does not panic.
	_ = err
}

func TestRedisTransport_ImplementsTransport(t *testing.T) {
	_, client := newTestRedisClient(t)
	tr, err := NewRedisTransport(client, "")
	if err != nil {
		t.Fatalf("NewRedisTransport: %v", err)
	}
	defer tr.Close()
	var _ Transport = tr
	var _ TransportHealthChecker = tr.(TransportHealthChecker)
}


