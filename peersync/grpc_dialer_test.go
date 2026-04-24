package peersync

import (
	"testing"

	"github.com/rbaliyan/config-server/client"
)

// TestGRPCDialer_ImplementsPeerDialer is a compile-time check that GRPCDialer
// satisfies the PeerDialer interface.
var _ PeerDialer = (*GRPCDialer)(nil)

// TestGRPCDialer_DialCachesConnection verifies that Dial for the same address
// returns the same config.Store on the second call (connection is cached).
func TestGRPCDialer_DialCachesConnection(t *testing.T) {
	d := NewGRPCDialer()

	first, err := d.Dial("localhost:9090")
	if err != nil {
		t.Fatalf("first Dial: %v", err)
	}

	second, err := d.Dial("localhost:9090")
	if err != nil {
		t.Fatalf("second Dial: %v", err)
	}

	if first != second {
		t.Error("Dial returned different stores for the same address; expected cached instance")
	}
}

// TestGRPCDialer_DialAfterClose_ReturnsError verifies that Dial returns an
// error after Close has been called.
func TestGRPCDialer_DialAfterClose_ReturnsError(t *testing.T) {
	d := NewGRPCDialer()

	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	_, err := d.Dial("localhost:9090")
	if err == nil {
		t.Error("Dial after Close: expected error, got nil")
	}
}

// TestGRPCDialer_Close_Empty verifies that Close with no cached connections
// returns nil.
func TestGRPCDialer_Close_Empty(t *testing.T) {
	d := NewGRPCDialer()

	if err := d.Close(); err != nil {
		t.Errorf("Close on empty dialer: expected nil, got %v", err)
	}
}

// TestGRPCDialer_Dial_EmptyAddr verifies that Dial("") returns an error
// because client.NewRemoteStore rejects empty addresses.
func TestGRPCDialer_Dial_EmptyAddr(t *testing.T) {
	d := NewGRPCDialer()
	defer d.Close() //nolint:errcheck

	_, err := d.Dial("")
	if err == nil {
		t.Error("Dial with empty addr: expected error, got nil")
	}
}

// TestGRPCDialer_DialDifferentAddrs verifies that Dial for different addresses
// returns distinct stores.
func TestGRPCDialer_DialDifferentAddrs(t *testing.T) {
	d := NewGRPCDialer()
	defer d.Close() //nolint:errcheck

	storeA, err := d.Dial("localhost:9090")
	if err != nil {
		t.Fatalf("Dial localhost:9090: %v", err)
	}

	storeB, err := d.Dial("localhost:9091")
	if err != nil {
		t.Fatalf("Dial localhost:9091: %v", err)
	}

	if storeA == storeB {
		t.Error("Dial for different addresses returned the same store")
	}
}

// TestGRPCDialer_NewGRPCDialer_WithOptions verifies that NewGRPCDialer accepts
// client.Option varargs without panicking.
func TestGRPCDialer_NewGRPCDialer_WithOptions(t *testing.T) {
	d := NewGRPCDialer(client.WithInsecure())
	if d == nil {
		t.Fatal("NewGRPCDialer returned nil")
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

// TestGRPCDialer_CloseTwice verifies that a second Close call does not panic.
// The second call sets closed=true again but conns is already nil.
func TestGRPCDialer_CloseTwice(t *testing.T) {
	d := NewGRPCDialer()

	_, err := d.Dial("localhost:9090")
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}

	if err := d.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	// Second Close should not panic; it sets closed=true on already-closed dialer.
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("second Close panicked: %v", r)
		}
	}()
	_ = d.Close()
}
