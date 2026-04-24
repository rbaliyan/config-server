package peersync_test

import (
	"context"
	"fmt"
	"sync"

	"github.com/rbaliyan/config"
	"github.com/rbaliyan/config/memory"
	"github.com/rbaliyan/config-server/peersync"
	goredis "github.com/redis/go-redis/v9"
)

// inProcTransport is a minimal thread-safe in-process transport for examples.
type inProcTransport struct {
	mu       sync.Mutex
	handlers []func([]byte)
}

func (t *inProcTransport) Publish(_ context.Context, p []byte) error {
	t.mu.Lock()
	handlers := make([]func([]byte), len(t.handlers))
	copy(handlers, t.handlers)
	t.mu.Unlock()
	for _, h := range handlers {
		h(p)
	}
	return nil
}

func (t *inProcTransport) Subscribe(_ context.Context, h func([]byte)) error {
	t.mu.Lock()
	t.handlers = append(t.handlers, h)
	t.mu.Unlock()
	return nil
}

func (t *inProcTransport) Close() error { return nil }

// inProcDialer implements PeerDialer by returning pre-registered stores.
type inProcDialer struct{ stores map[string]config.Store }

func (d *inProcDialer) Dial(addr string) (config.Store, error) {
	if s, ok := d.stores[addr]; ok {
		return s, nil
	}
	return nil, fmt.Errorf("no store registered for addr %q", addr)
}

// memOwnershipStore is an in-memory OwnershipStore used in examples.
type memOwnershipStore struct {
	mu   sync.Mutex
	data map[string]string // namespace → nodeID
}

func (s *memOwnershipStore) LoadOwned(_ context.Context, nodeID string) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var ns []string
	for namespace, id := range s.data {
		if id == nodeID {
			ns = append(ns, namespace)
		}
	}
	return ns, nil
}

func (s *memOwnershipStore) SaveOwner(_ context.Context, namespace, nodeID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.data == nil {
		s.data = make(map[string]string)
	}
	s.data[namespace] = nodeID
	return nil
}

func (s *memOwnershipStore) DeleteOwner(_ context.Context, namespace string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.data, namespace)
	return nil
}

// ExampleNew shows how to wire two SyncStore nodes on a shared in-process
// transport. nodeA claims namespace "app"; nodeB's writes are forwarded to
// nodeA via PeerDialer.
func ExampleNew() {
	ctx := context.Background()
	tr := &inProcTransport{}

	storeA := memory.NewStore()
	storeB := memory.NewStore()

	// dialerB resolves "nodeA:9000" to nodeA's local store (in-process shortcut;
	// in production this would be a RemoteStore gRPC client).
	dialerB := &inProcDialer{stores: map[string]config.Store{"nodeA:9000": storeA}}

	// Errors are omitted below for brevity; production code must handle them.
	nodeA, _ := peersync.New(storeA, peersync.Member{ID: "nodeA", Addr: "nodeA:9000"}, tr)
	nodeB, _ := peersync.New(storeB, peersync.Member{ID: "nodeB", Addr: "nodeB:9000"}, tr,
		peersync.WithPeerDialer(dialerB),
	)

	_ = nodeA.Connect(ctx)
	defer nodeA.Close(ctx)

	_ = nodeB.Connect(ctx)
	defer nodeB.Close(ctx)

	// Claim makes nodeA the persistent owner of "app". The ownership is
	// gossiped to nodeB so subsequent Set calls on nodeB are forwarded.
	_ = nodeA.Claim(ctx, "app")

	// OwnerOf returns the recorded routing entry and whether one exists.
	// To check whether the owner is currently alive, use HasMember.
	owner, found := nodeA.OwnerOf("app")
	fmt.Printf("owner of app: %s (found=%v)\n", owner, found)
	// Output: owner of app: nodeA (found=true)
}

// ExampleSyncStore_Pin shows how to temporarily route a namespace to a
// specific node, bypassing the consistent-hash ring. Pin returns an error if
// the target node is not currently in the ring; call HasMember to pre-validate.
// Pins propagate via gossip but are not persisted — they are lost when all
// cluster nodes restart. Use Claim for persistence.
func ExampleSyncStore_Pin() {
	ctx := context.Background()
	tr := &inProcTransport{}

	storeA := memory.NewStore()
	// Errors are omitted below for brevity; production code must handle them.
	nodeA, _ := peersync.New(storeA, peersync.Member{ID: "nodeA", Addr: "nodeA:9000"}, tr)
	_ = nodeA.Connect(ctx)
	defer nodeA.Close(ctx)

	// Pin overrides the hash ring to route "session" to "nodeA" explicitly.
	// The target must be a known member (use HasMember to pre-validate).
	_ = nodeA.Pin("session", "nodeA")

	owner, found := nodeA.OwnerOf("session")
	fmt.Printf("session owner: %s (found=%v)\n", owner, found)

	// Unpin restores hash-ring routing for the namespace.
	nodeA.Unpin("session")
	fmt.Printf("after unpin, hasPin=%v\n", nodeA.HasMember("nodeA"))
	// Output:
	// session owner: nodeA (found=true)
	// after unpin, hasPin=true
}

// ExampleSyncStore_Claim shows how to persistently assign a namespace to a
// node using an OwnershipStore. On the next Connect, the node reloads its
// claimed namespaces and re-announces them before the first gossip broadcast.
func ExampleSyncStore_Claim() {
	ctx := context.Background()
	tr := &inProcTransport{}

	store := memory.NewStore()
	ownership := &memOwnershipStore{}

	// Errors are omitted below for brevity; production code must handle them.
	node, _ := peersync.New(store, peersync.Member{ID: "node1", Addr: "node1:9000"}, tr,
		peersync.WithOwnershipStore(ownership),
	)
	_ = node.Connect(ctx)
	defer node.Close(ctx)

	// Claim persists "billing" ownership to the OwnershipStore so that if this
	// node restarts, it re-announces ownership before any other gossip fires.
	_ = node.Claim(ctx, "billing")

	owner, found := node.OwnerOf("billing")
	fmt.Printf("billing owner: %s (found=%v)\n", owner, found)
	// Output: billing owner: node1 (found=true)
}

// ExampleNewRedisTransport demonstrates wiring a Redis pub/sub transport.
// This example requires a live Redis instance and is not executed during tests.
func ExampleNewRedisTransport() {
	rdb := goredis.NewClient(&goredis.Options{Addr: "localhost:6379"})

	// Errors are omitted below for brevity; production code must handle them.
	//
	// Pass "" to use the built-in channel name "config:sync". Use an explicit
	// channel when multiple independent clusters share one Redis instance to
	// prevent cross-cluster gossip pollution.
	tr, _ := peersync.NewRedisTransport(rdb, "prod:config:sync")
	defer tr.Close()

	store := memory.NewStore()
	node, _ := peersync.New(store, peersync.Member{ID: "node1", Addr: "node1:9000"}, tr)
	ctx := context.Background()
	_ = node.Connect(ctx)
	defer node.Close(ctx)
}
