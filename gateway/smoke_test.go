package gateway

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/rbaliyan/config-server/service"
	"github.com/rbaliyan/config/memory"
)

// smokeTimeout bounds smoke-test bodies so a wedged dependency fails fast
// instead of hanging until the package test timeout.
const smokeTimeout = 10 * time.Second

// TestSmoke_GatewayHTTPWriteRoundTrip exercises the full HTTP/JSON write path
// end-to-end over the in-process gateway: POST a value, GET it back, then
// DELETE and confirm a follow-up GET returns 404. This closes the gap where
// only the read (GET) path was smoke-tested.
//
// The Set RPC uses `body: "*"`, so namespace/key come from the URL path and
// the JSON body carries the proto SetRequest fields. The `value` field is
// proto `bytes`, which marshals as base64 in proto-JSON; for the default
// "json" codec the decoded bytes are the JSON encoding of the value (a quoted
// string for a string value).
func TestSmoke_GatewayHTTPWriteRoundTrip(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), smokeTimeout)
	t.Cleanup(cancel)

	store := memory.NewStore()
	if err := store.Connect(ctx); err != nil {
		t.Fatalf("store.Connect: %v", err)
	}
	t.Cleanup(func() { _ = store.Close(context.Background()) })

	svc, err := service.NewService(store, service.WithSecurityGuard(service.AllowAll()))
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}

	handler, err := NewInProcessHandler(ctx, svc)
	if err != nil {
		t.Fatalf("NewInProcessHandler: %v", err)
	}
	t.Cleanup(func() { _ = handler.Close() })

	const (
		ns  = "smoke-ns"
		key = "greeting"
	)
	path := "/v1/namespaces/" + ns + "/keys/" + key

	// The JSON value bytes for the string "smoke-value" via the json codec.
	valueBytes := []byte(`"smoke-value"`)
	wantValueB64 := base64.StdEncoding.EncodeToString(valueBytes)

	// POST (Set): body carries the base64-encoded value bytes.
	body, err := json.Marshal(map[string]any{"value": wantValueB64})
	if err != nil {
		t.Fatalf("marshal request body: %v", err)
	}
	postReq := httptest.NewRequest(http.MethodPost, path, strings.NewReader(string(body)))
	postReq.Header.Set("Content-Type", "application/json")
	postRec := httptest.NewRecorder()
	handler.ServeHTTP(postRec, postReq)
	if postRec.Code != http.StatusOK {
		t.Fatalf("POST status = %d, want 200; body: %s", postRec.Code, postRec.Body.String())
	}

	// The Set response echoes the stored entry; its value must round-trip.
	if got := decodeEntryValue(t, postRec.Body.Bytes()); got != wantValueB64 {
		t.Errorf("POST response value = %q, want %q", got, wantValueB64)
	}

	// GET it back: body must contain the same value.
	getRec := doGet(t, handler, path)
	if getRec.Code != http.StatusOK {
		t.Fatalf("GET status = %d, want 200; body: %s", getRec.Code, getRec.Body.String())
	}
	if !strings.Contains(getRec.Body.String(), wantValueB64) {
		t.Errorf("GET body %q does not contain value %q", getRec.Body.String(), wantValueB64)
	}
	if got := decodeEntryValue(t, getRec.Body.Bytes()); got != wantValueB64 {
		t.Errorf("GET response value = %q, want %q", got, wantValueB64)
	}

	// DELETE it.
	delReq := httptest.NewRequest(http.MethodDelete, path, nil)
	delRec := httptest.NewRecorder()
	handler.ServeHTTP(delRec, delReq)
	if delRec.Code != http.StatusOK {
		t.Fatalf("DELETE status = %d, want 200; body: %s", delRec.Code, delRec.Body.String())
	}

	// GET after DELETE must be 404 (ErrNotFound → NotFound → HTTP 404).
	gone := doGet(t, handler, path)
	if gone.Code != http.StatusNotFound {
		t.Errorf("GET after DELETE status = %d, want 404; body: %s", gone.Code, gone.Body.String())
	}
}

// doGet issues an in-process GET and returns the recorder.
func doGet(t *testing.T, handler http.Handler, path string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, path, nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	return rec
}

// decodeEntryValue extracts the base64 `value` field from a Get/Set JSON
// response of shape {"entry":{"value":"<base64>", ...}}.
func decodeEntryValue(t *testing.T, body []byte) string {
	t.Helper()
	var resp struct {
		Entry struct {
			Value string `json:"value"`
		} `json:"entry"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatalf("unmarshal response %q: %v", string(body), err)
	}
	return resp.Entry.Value
}
