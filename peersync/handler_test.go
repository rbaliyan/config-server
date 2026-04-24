package peersync

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

// TestNewStatusHandler_ContentType verifies that the handler sets
// Content-Type: application/json on the response.
func TestNewStatusHandler_ContentType(t *testing.T) {
	tr := &memTransport{}
	s := newTestStore(t, "nodeA", tr)

	h := NewStatusHandler(s)
	rec := httptest.NewRecorder()
	req, _ := http.NewRequest(http.MethodGet, "/debug/ring", nil)
	h.ServeHTTP(rec, req)

	if got := rec.Header().Get("Content-Type"); got != "application/json" {
		t.Errorf("Content-Type = %q, want %q", got, "application/json")
	}
}

// TestNewStatusHandler_StatusOK verifies that the handler returns HTTP 200.
func TestNewStatusHandler_StatusOK(t *testing.T) {
	tr := &memTransport{}
	s := newTestStore(t, "nodeA", tr)

	h := NewStatusHandler(s)
	rec := httptest.NewRecorder()
	req, _ := http.NewRequest(http.MethodGet, "/debug/ring", nil)
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusOK)
	}
}

// TestNewStatusHandler_SelfField verifies that the "self" field in the JSON
// response matches the member used to create the store.
func TestNewStatusHandler_SelfField(t *testing.T) {
	tr := &memTransport{}
	s := newTestStore(t, "nodeA", tr)

	h := NewStatusHandler(s)
	rec := httptest.NewRecorder()
	req, _ := http.NewRequest(http.MethodGet, "/debug/ring", nil)
	h.ServeHTTP(rec, req)

	var resp statusResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	if resp.Self.ID != "nodeA" {
		t.Errorf("self.id = %q, want %q", resp.Self.ID, "nodeA")
	}
	if resp.Self.Addr != "nodeA:9000" {
		t.Errorf("self.addr = %q, want %q", resp.Self.Addr, "nodeA:9000")
	}
}

// TestNewStatusHandler_MembersField verifies that the "members" array is
// present and non-empty (at least the self node).
func TestNewStatusHandler_MembersField(t *testing.T) {
	tr := &memTransport{}
	s := newTestStore(t, "nodeA", tr)

	h := NewStatusHandler(s)
	rec := httptest.NewRecorder()
	req, _ := http.NewRequest(http.MethodGet, "/debug/ring", nil)
	h.ServeHTTP(rec, req)

	var resp statusResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	if len(resp.Members) == 0 {
		t.Error("members field is empty, expected at least the self node")
	}
	found := false
	for _, m := range resp.Members {
		if m.ID == "nodeA" {
			found = true
			break
		}
	}
	if !found {
		t.Error("self node not found in members list")
	}
}

// TestNewStatusHandler_EpochField verifies that the "epoch" field is present
// and is a non-negative integer.
func TestNewStatusHandler_EpochField(t *testing.T) {
	tr := &memTransport{}
	s := newTestStore(t, "nodeA", tr)

	h := NewStatusHandler(s)
	rec := httptest.NewRecorder()
	req, _ := http.NewRequest(http.MethodGet, "/debug/ring", nil)
	h.ServeHTTP(rec, req)

	var resp statusResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	if resp.Epoch < 0 {
		t.Errorf("epoch = %d, want >= 0", resp.Epoch)
	}
}

// TestNewStatusHandler_OverridesField verifies that the "overrides" field
// reflects active pins on the ring.
func TestNewStatusHandler_OverridesField(t *testing.T) {
	tr := &memTransport{}
	s := newTestStore(t, "nodeA", tr)

	// Pin a namespace to self — this is always valid.
	if err := s.Pin("payments", "nodeA"); err != nil {
		t.Fatalf("Pin: %v", err)
	}

	h := NewStatusHandler(s)
	rec := httptest.NewRecorder()
	req, _ := http.NewRequest(http.MethodGet, "/debug/ring", nil)
	h.ServeHTTP(rec, req)

	var resp statusResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	if resp.Overrides == nil {
		t.Fatal("overrides is nil, expected map with payments entry")
	}
	if got := resp.Overrides["payments"]; got != "nodeA" {
		t.Errorf("overrides[payments] = %q, want %q", got, "nodeA")
	}
}

// TestNewStatusHandler_ValidJSON verifies that the response body is valid JSON
// with the expected top-level keys.
func TestNewStatusHandler_ValidJSON(t *testing.T) {
	tr := &memTransport{}
	s := newTestStore(t, "nodeA", tr)

	h := NewStatusHandler(s)
	rec := httptest.NewRecorder()
	req, _ := http.NewRequest(http.MethodGet, "/debug/ring", nil)
	h.ServeHTTP(rec, req)

	var raw map[string]json.RawMessage
	if err := json.NewDecoder(rec.Body).Decode(&raw); err != nil {
		t.Fatalf("response is not valid JSON: %v", err)
	}
	for _, key := range []string{"self", "epoch", "members"} {
		if _, ok := raw[key]; !ok {
			t.Errorf("response missing key %q", key)
		}
	}
}

// TestSyncStore_Self verifies that Self() returns the correct member.
func TestSyncStore_Self(t *testing.T) {
	tr := &memTransport{}
	s := newTestStore(t, "nodeA", tr)

	got := s.Self()
	if got.ID != "nodeA" {
		t.Errorf("Self().ID = %q, want %q", got.ID, "nodeA")
	}
	if got.Addr != "nodeA:9000" {
		t.Errorf("Self().Addr = %q, want %q", got.Addr, "nodeA:9000")
	}
}
