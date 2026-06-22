package gateway

import (
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
)

// FuzzParseDiffParams fuzzes parseDiffParams, which feeds the version-diff
// endpoint (/v1/namespaces/{namespace}/keys/{key}/diff?v1=&v2=). The namespace
// and key come from untrusted path values and v1/v2 from untrusted query
// parameters, so the parser processes fully attacker-controlled input. The
// invariants are:
//
//   - parseDiffParams must never panic on any combination of inputs.
//   - When it returns a nil error, the returned namespace/key must equal the
//     supplied path values (no silent substitution) and v1/v2 must round-trip:
//     re-parsing the supplied query strings with strconv.ParseInt must yield the
//     exact returned int64 values. This guards against the handler diffing a
//     different version pair than the request named.
//   - When namespace or key is empty, the parse must fail (the handler relies on
//     that to reject malformed routes before touching the store).
func FuzzParseDiffParams(f *testing.F) {
	// Realistic route shapes.
	f.Add("prod", "app/db/host", "1", "2")
	f.Add("staging", "feature/flag", "0", "10")
	f.Add("ns", "key", "100", "200")
	// Empty components (must error).
	f.Add("", "", "", "")
	f.Add("", "key", "1", "2")
	f.Add("ns", "", "1", "2")
	// Non-numeric versions (must error).
	f.Add("ns", "key", "abc", "2")
	f.Add("ns", "key", "1", "xyz")
	f.Add("ns", "key", "", "")
	// Negative / signed.
	f.Add("ns", "key", "-1", "-2")
	f.Add("ns", "key", "+5", "+6")
	// Overflow ParseInt (int64 range).
	f.Add("ns", "key", "99999999999999999999999999", "1")
	f.Add("ns", "key", "9223372036854775807", "9223372036854775808")
	// Whitespace / formatting abuse.
	f.Add("ns", "key", "  5  ", "6")
	f.Add("ns", "key", "5\n", "6")
	f.Add("ns", "key", "0x10", "0o7")
	// Traversal / injection in path values.
	f.Add("../../etc", "passwd", "1", "2")
	f.Add("ns", "key/../../../secret", "1", "2")
	f.Add("ns\ninjection", "key", "1", "2")
	// Unicode.
	f.Add("日本語", "鍵", "1", "2")
	f.Add("ns", "ключ", "1", "2")
	// Oversized.
	f.Add(strings.Repeat("n", 10000), strings.Repeat("k", 10000), "1", "2")

	f.Fuzz(func(t *testing.T, namespace, key, v1, v2 string) {
		// Build a request whose query carries v1/v2 and whose path values carry
		// namespace/key, mirroring how the gateway ServeMux populates them.
		// url.Values.Encode handles escaping so arbitrary fuzz bytes are legal.
		req := httptest.NewRequest("GET", "/v1/diff", nil)
		q := req.URL.Query()
		q.Set("v1", v1)
		q.Set("v2", v2)
		req.URL.RawQuery = q.Encode()
		req.SetPathValue("namespace", namespace)
		req.SetPathValue("key", key)

		// Invariant 1: never panics.
		gotNS, gotKey, gotV1, gotV2, err := parseDiffParams(req)

		// Invariant 2: empty namespace or key must fail.
		if namespace == "" || key == "" {
			if err == nil {
				t.Fatalf("parseDiffParams accepted empty namespace=%q key=%q", namespace, key)
			}
			return
		}

		if err != nil {
			// On error the numeric outputs are unspecified; only crash-freedom
			// and the empty-component rule matter here.
			return
		}

		// Invariant 3: path values echoed verbatim (no substitution).
		if gotNS != namespace {
			t.Fatalf("namespace not preserved: %q -> %q", namespace, gotNS)
		}
		if gotKey != key {
			t.Fatalf("key not preserved: %q -> %q", key, gotKey)
		}

		// Invariant 4: v1/v2 round-trip — the returned ints must match an
		// independent parse of the same query strings, so the handler diffs the
		// exact version pair the caller named.
		wantV1, err1 := strconv.ParseInt(req.URL.Query().Get("v1"), 10, 64)
		wantV2, err2 := strconv.ParseInt(req.URL.Query().Get("v2"), 10, 64)
		if err1 != nil || err2 != nil {
			t.Fatalf("parseDiffParams returned nil error but query reparse failed: v1err=%v v2err=%v", err1, err2)
		}
		if gotV1 != wantV1 {
			t.Fatalf("v1 mismatch: got %d, reparse %d (raw %q)", gotV1, wantV1, v1)
		}
		if gotV2 != wantV2 {
			t.Fatalf("v2 mismatch: got %d, reparse %d (raw %q)", gotV2, wantV2, v2)
		}
	})
}
