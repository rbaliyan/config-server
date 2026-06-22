package gateway

import (
	"strings"
	"testing"
)

// FuzzResourceFromPath fuzzes the security-relevant HTTP authorization path
// parser resourceFromPath. The parser maps a request path onto a
// service.Resource{Namespace, Key} that resource-scoped authorization policies
// fire on, so an attacker who can make the parser derive a different
// namespace/key than the route actually serves could impersonate another
// resource. The oracle therefore asserts both crash-freedom and several
// structural invariants on the derived Resource.
func FuzzResourceFromPath(f *testing.F) {
	// Real route shapes from the gateway (see CLAUDE.md HTTP Routes table).
	f.Add("/v1/namespaces/prod/keys/app/db/host")
	f.Add("/v1/namespaces/prod/keys/app/db/host/versions")
	f.Add("/v1/namespaces/prod/keys/app/db/host/diff")
	f.Add("/v1/namespaces/prod/keys/simplekey")
	f.Add("/v1/namespaces/prod/keys")
	f.Add("/v1/namespaces/prod/snapshot")
	f.Add("/v1/namespaces/prod/access")
	f.Add("/v1/namespaces")
	f.Add("/v1/aliases/myalias")
	f.Add("/v1/aliases")
	f.Add("/v1/codecs")
	f.Add("/v1/watch")

	// Adversarial inputs: traversal, encoded traversal, double slashes,
	// empty, unicode, marker-only, repeated markers.
	f.Add("")
	f.Add("/")
	f.Add("..")
	f.Add("/v1/namespaces/../keys/../etc/passwd")
	f.Add("/v1/namespaces/%2e%2e/keys/%2e%2e")
	f.Add("/v1/namespaces//keys//")
	f.Add("/v1/namespaces/ns/keys/")
	f.Add("/v1/namespaces/keys/keys/keys")
	f.Add("/v1/aliases/a/b")
	f.Add("/v1/aliases//")
	f.Add("namespaces/x/keys/y")
	f.Add("/v1/namespaces/日本語/keys/键")
	f.Add("/v1/namespaces/ns/keys/k/versions/versions")
	f.Add("/v1/namespaces/ns/keys/k/diff/diff")
	f.Add(strings.Repeat("/v1/namespaces/n/keys/k", 50))

	f.Fuzz(func(t *testing.T, path string) {
		// Invariant 1: never panics on arbitrary input.
		res := resourceFromPath(path)

		// Invariant 2: the derived namespace and key must be substrings of the
		// input path. The parser only ever slices the path (never synthesizes
		// new bytes), so anything else signals a parsing bug that could let one
		// resource impersonate another.
		if res.Namespace != "" && !strings.Contains(path, res.Namespace) {
			t.Fatalf("namespace %q not a substring of path %q", res.Namespace, path)
		}
		if res.Key != "" && !strings.Contains(path, res.Key) {
			t.Fatalf("key %q not a substring of path %q", res.Key, path)
		}

		// Invariant 3 (determinism): parsing the same path twice must yield the
		// identical Resource. A non-deterministic parse would mean an
		// authorization decision could differ from the decision the handler
		// later acts on for the very same request.
		res2 := resourceFromPath(path)
		if res2 != res {
			t.Fatalf("resourceFromPath not deterministic: %+v vs %+v (path %q)", res, res2, path)
		}

		// Invariant 4 (stability + idempotence): when a Resource is derived from
		// the canonical /v1/namespaces/{ns}/keys/{key} family, reconstructing
		// that canonical path from the derived identifiers and re-parsing must
		// yield the identical Resource. This is the core impersonation guard: it
		// proves the parse is a stable fixed point, so an attacker cannot craft a
		// path that parses to resource A on the authorization check but resource
		// B once normalized. We restrict to inputs whose derived components do
		// not themselves re-embed routing markers, where the function is
		// explicitly best-effort.
		if res.Namespace != "" && res.Key != "" &&
			!strings.Contains(res.Namespace, "/keys/") &&
			!strings.Contains(res.Key, "/keys/") &&
			!strings.HasSuffix(res.Key, "/versions") &&
			!strings.HasSuffix(res.Key, "/diff") {
			canonical := "/v1/namespaces/" + res.Namespace + "/keys/" + res.Key
			reparsed := resourceFromPath(canonical)
			if reparsed.Namespace != res.Namespace {
				t.Fatalf("namespace not idempotent: %q -> %q via %q",
					res.Namespace, reparsed.Namespace, canonical)
			}
			if reparsed.Key != res.Key {
				t.Fatalf("key not idempotent: %q -> %q via %q",
					res.Key, reparsed.Key, canonical)
			}
		}

		// Invariant 5 (no silent decoding): resourceFromPath operates on the raw
		// (still percent-encoded) path. It must never decode %2e%2e into ".." —
		// if it did, an encoded-traversal payload could collapse onto a different
		// resource than the raw path names. Since the outputs are slices of the
		// raw path (invariant 2), an encoded sequence in the input must remain
		// encoded in any derived component.
		if strings.Contains(path, "%2e") || strings.Contains(path, "%2E") {
			if strings.Contains(res.Namespace, "..") || strings.Contains(res.Key, "..") {
				// Only a defect if the ".." did NOT come from a literal ".." in
				// the raw path (i.e. it was decoded from %2e). Guard on the raw
				// path not already containing the literal "..".
				if !strings.Contains(path, "..") {
					t.Fatalf("encoded traversal decoded into component (path %q -> ns %q key %q)",
						path, res.Namespace, res.Key)
				}
			}
		}
	})
}
