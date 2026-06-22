package service

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/rbaliyan/config"
	configpb "github.com/rbaliyan/config-server/proto/config/v1"
	"github.com/rbaliyan/config/memory"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// assertProperStatus asserts that, when an RPC returns an error, that error is
// a well-formed gRPC status with a recognised code (never a bare Go error and
// never an OK status paired with a nil response). This is the shared oracle for
// the service-level targets: a SecurityGuard=AllowAll server must only ever
// surface mapped gRPC errors, so anything else (e.g. a raw fmt.Errorf leaking
// out of a handler) is a defect.
func assertProperStatus(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		return
	}
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("RPC returned non-status error: %v", err)
	}
	switch st.Code() {
	case codes.OK:
		t.Fatalf("error carried OK status code: %v", err)
	case codes.Unknown:
		t.Fatalf("error carried Unknown status code (unmapped error?): %v", err)
	}
}

func FuzzServiceGet(f *testing.F) {
	f.Add("production", "app/database/host")
	f.Add("", "")
	f.Add("ns", "key")
	f.Add("special!ns", "key/../traversal")
	f.Add("prod", "/leading-slash")
	// Realistic.
	f.Add("default", "feature.flag.enabled")
	f.Add("staging", "service/auth/token")
	f.Add("prod", "a/b/c/d/e/f")
	// Adversarial: traversal, injection, control chars, unicode, oversized,
	// empty halves, slashes.
	f.Add("ns", "../../etc/passwd")
	f.Add("../secret", "key")
	f.Add("ns\ninjection", "key")
	f.Add("ns", "key\x00null")
	f.Add("日本語", "鍵")
	f.Add("ns", "ключ/значение")
	f.Add("ns", "key with spaces")
	f.Add("ns", "")
	f.Add("", "key")
	f.Add("ns", "trailing/")
	f.Add("ns", "//double//slash//")
	f.Add(strings.Repeat("n", 5000), strings.Repeat("k", 5000))
	f.Add("ns", "emoji-🔥-key")

	ctx := context.Background()
	store := memory.NewStore()
	if err := store.Connect(ctx); err != nil {
		f.Fatal(err)
	}
	svc, err := NewService(store, WithSecurityGuard(AllowAll()))
	if err != nil {
		f.Fatal(err)
	}

	f.Fuzz(func(t *testing.T, namespace, key string) {
		resp, err := svc.Get(context.Background(), &configpb.GetRequest{
			Namespace: namespace,
			Key:       key,
		})
		// Oracle: errors must be proper gRPC statuses.
		assertProperStatus(t, err)
		// Oracle: success implies a non-nil response whose entry echoes the
		// requested coordinates (no silent key substitution).
		if err == nil {
			if resp == nil {
				t.Fatal("Get returned nil response and nil error")
			}
			if e := resp.GetEntry(); e != nil {
				if e.GetNamespace() != namespace {
					t.Fatalf("entry namespace %q != requested %q", e.GetNamespace(), namespace)
				}
				if e.GetKey() != key {
					t.Fatalf("entry key %q != requested %q", e.GetKey(), key)
				}
			}
		}
	})
}

func FuzzServiceSet(f *testing.F) {
	f.Add("production", "app/key", []byte(`"hello"`), "json")
	f.Add("", "", []byte{}, "")
	f.Add("ns", "key", []byte(`{"nested":true}`), "json")
	f.Add("ns", "key", []byte(`invalid json`), "json")
	f.Add("ns", "key", []byte(`value: true`), "yaml")
	// Realistic values across codecs.
	f.Add("prod", "count", []byte(`42`), "json")
	f.Add("prod", "ratio", []byte(`3.14`), "json")
	f.Add("prod", "flag", []byte(`true`), "json")
	f.Add("prod", "list", []byte(`[1,2,3]`), "json")
	f.Add("prod", "obj", []byte(`{"a":{"b":[1,2]}}`), "json")
	f.Add("prod", "yaml-map", []byte("a: 1\nb: 2\n"), "yaml")
	// Adversarial: malformed payloads, codec confusion, binary, oversized,
	// injection, unicode, empty/unknown codec, control chars.
	f.Add("ns", "k", []byte(`{"unterminated":`), "json")
	f.Add("ns", "k", []byte("\x00\x01\x02\xff"), "raw")
	f.Add("ns", "k", []byte(`{"a":1}`), "unknown-codec")
	f.Add("ns", "k", []byte(`null`), "json")
	f.Add("ns", "k", []byte("a: [unterminated"), "yaml")
	f.Add("ns\ninject", "k", []byte(`"v"`), "json")
	f.Add("ns", "../traversal", []byte(`"v"`), "json")
	f.Add("日本語", "鍵", []byte(`"値"`), "json")
	f.Add("ns", "k", bytes.Repeat([]byte("x"), 100000), "raw")
	f.Add("ns", "k", []byte(`"deep"`), strings.Repeat("c", 1000))
	f.Add("ns", "k", []byte{}, "json")

	f.Fuzz(func(t *testing.T, namespace, key string, value []byte, codec string) {
		// A fresh store per iteration so the "no partial write on error" oracle
		// is sound: the only way a value can exist for (namespace,key) after the
		// Set under test is if that Set itself wrote it.
		ctx := context.Background()
		store := memory.NewStore()
		if err := store.Connect(ctx); err != nil {
			t.Fatal(err)
		}
		svc, err := NewService(store, WithSecurityGuard(AllowAll()))
		if err != nil {
			t.Fatal(err)
		}

		resp, err := svc.Set(ctx, &configpb.SetRequest{
			Namespace: namespace,
			Key:       key,
			Value:     value,
			Codec:     codec,
		})
		assertProperStatus(t, err)

		if err != nil {
			// Oracle: no partial write on error. On a fresh store, a failed Set
			// must not leave any readable value behind for those coordinates.
			got, gerr := svc.Get(ctx, &configpb.GetRequest{
				Namespace: namespace,
				Key:       key,
			})
			if gerr == nil && got.GetEntry() != nil && len(got.GetEntry().GetValue()) > 0 {
				t.Fatalf("Set failed but left a stored value for %q/%q: %q",
					namespace, key, got.GetEntry().GetValue())
			}
			return
		}

		// Oracle: success implies a non-nil response echoing coordinates.
		if resp == nil {
			t.Fatal("Set returned nil response and nil error")
		}
		if e := resp.GetEntry(); e != nil {
			if e.GetNamespace() != namespace || e.GetKey() != key {
				t.Fatalf("Set echoed %q/%q != requested %q/%q",
					e.GetNamespace(), e.GetKey(), namespace, key)
			}
		}
	})
}

func FuzzServiceList(f *testing.F) {
	f.Add("production", "app/", int32(100), "")
	f.Add("", "", int32(0), "")
	f.Add("ns", "prefix", int32(-1), "cursor123")
	// Realistic.
	f.Add("production", "app/db", int32(10), "")
	f.Add("production", "", int32(50), "")
	f.Add("production", "app/", int32(1), "")
	// Adversarial: limit extremes, malformed cursors, injection, traversal,
	// unicode, oversized prefix, control chars.
	f.Add("production", "app/", int32(2147483647), "")  // int32 max limit
	f.Add("production", "app/", int32(-2147483648), "") // int32 min limit
	f.Add("production", "app/", int32(0), "not-base64!!!")
	f.Add("production", "app/", int32(10), "../../escape")
	f.Add("production", "../traversal", int32(10), "")
	f.Add("production\ninject", "app/", int32(10), "")
	f.Add("日本語", "鍵/", int32(10), "")
	f.Add("production", strings.Repeat("p", 5000), int32(10), "")
	f.Add("production", "app/", int32(10), strings.Repeat("c", 5000))
	f.Add("production", "app/\x00", int32(10), "")

	ctx := context.Background()
	store := memory.NewStore()
	if err := store.Connect(ctx); err != nil {
		f.Fatal(err)
	}
	svc, err := NewService(store, WithSecurityGuard(AllowAll()))
	if err != nil {
		f.Fatal(err)
	}

	// Pre-populate some data
	_, _ = store.Set(context.Background(), "production", "app/db", config.NewValue("localhost"))
	_, _ = store.Set(context.Background(), "production", "app/port", config.NewValue(5432))

	f.Fuzz(func(t *testing.T, namespace, prefix string, limit int32, cursor string) {
		resp, err := svc.List(context.Background(), &configpb.ListRequest{
			Namespace: namespace,
			Prefix:    prefix,
			Limit:     limit,
			Cursor:    cursor,
		})
		assertProperStatus(t, err)
		if err != nil {
			return
		}
		if resp == nil {
			t.Fatal("List returned nil response and nil error")
		}
		// Oracle: every returned entry matches the requested namespace and the
		// requested prefix — List must never leak entries from another
		// namespace or outside the prefix filter.
		for _, e := range resp.GetEntries() {
			if e == nil {
				continue
			}
			if e.GetNamespace() != namespace {
				t.Fatalf("List(%q) returned entry from namespace %q", namespace, e.GetNamespace())
			}
			if prefix != "" && !hasPrefix(e.GetKey(), prefix) {
				t.Fatalf("List(prefix=%q) returned key %q outside prefix", prefix, e.GetKey())
			}
		}
	})
}

func hasPrefix(s, prefix string) bool {
	return len(s) >= len(prefix) && s[:len(prefix)] == prefix
}

// FuzzValueToProtoRoundTrip is a round-trip oracle on the proto<->Value
// conversion (service.valueToProto and its client-side inverse semantics). A
// config.Value built from fuzzed bytes+codec is converted to a proto Entry and
// the conversion must preserve type, codec, namespace, key, and the marshaled
// payload with full fidelity, and must never panic. valueToProto is the
// trusted boundary the gateway uses to serialize stored values onto the wire,
// so any silent corruption here is observable by every client.
func FuzzValueToProtoRoundTrip(f *testing.F) {
	f.Add("ns", "key", []byte(`"hello"`), "json", int32(config.TypeString))
	f.Add("", "", []byte{}, "", int32(config.TypeUnknown))
	f.Add("prod", "app/db", []byte(`{"a":1}`), "json", int32(config.TypeMapStringInt))
	f.Add("ns", "k", []byte("\x00\x01\x02binary"), "raw", int32(0))
	f.Add("ns", "k", []byte("value: true"), "yaml", int32(config.TypeBool))
	f.Add("ns/with/slashes", "key/with/slashes", []byte("123"), "json", int32(config.TypeInt))
	f.Add("ns", "k", bytes.Repeat([]byte("x"), 4096), "raw", int32(99))
	// More realistic type/codec combos.
	f.Add("prod", "n", []byte("3.14"), "json", int32(config.TypeFloat))
	f.Add("prod", "flag", []byte("true"), "json", int32(config.TypeBool))
	f.Add("prod", "list", []byte("[1,2,3]"), "json", int32(config.TypeUnknown))
	// Adversarial: negative/huge type enums, empty codec, binary payloads,
	// unicode, injection, oversized, control chars.
	f.Add("ns", "k", []byte("v"), "raw", int32(-1))
	f.Add("ns", "k", []byte("v"), "raw", int32(2147483647))
	f.Add("ns", "k", []byte("v"), "raw", int32(-2147483648))
	f.Add("ns", "k", []byte{}, "", int32(0))
	f.Add("ns\ninject", "k", []byte("v"), "raw", int32(0))
	f.Add("ns", "../traversal", []byte("v"), "raw", int32(0))
	f.Add("日本語", "鍵", []byte("値"), "raw", int32(config.TypeString))
	f.Add("ns", "k", []byte("\xff\xfe\x00\x01"), "raw", int32(0))
	f.Add("ns", "k", bytes.Repeat([]byte("y"), 100000), "raw", int32(0))
	f.Add("ns", "k", []byte("v"), strings.Repeat("c", 1000), int32(0))

	ctx := context.Background()

	f.Fuzz(func(t *testing.T, namespace, key string, raw []byte, codec string, typ int32) {
		// Build a raw value: bytes stored verbatim, returned as-is by Marshal.
		// This isolates the conversion logic from codec-specific marshalling.
		val := config.NewRawValue(raw, codec, config.WithValueType(config.Type(typ)))

		entry, err := valueToProto(ctx, namespace, key, val)
		if err != nil {
			// A raw value's Marshal never errors, so valueToProto should not
			// either; if it does it must at least not return a partial entry.
			if entry != nil {
				t.Fatalf("valueToProto returned both entry and error: %v", err)
			}
			return
		}
		if entry == nil {
			t.Fatal("valueToProto returned nil entry and nil error")
		}

		// Fidelity: coordinates preserved exactly.
		if entry.GetNamespace() != namespace {
			t.Fatalf("namespace not preserved: %q -> %q", namespace, entry.GetNamespace())
		}
		if entry.GetKey() != key {
			t.Fatalf("key not preserved: %q -> %q", key, entry.GetKey())
		}

		// Fidelity: codec preserved exactly.
		if entry.GetCodec() != val.Codec() {
			t.Fatalf("codec not preserved: %q -> %q", val.Codec(), entry.GetCodec())
		}

		// Fidelity: payload preserved byte-for-byte (raw value round-trips).
		marshaled, merr := val.Marshal(ctx)
		if merr != nil {
			t.Fatalf("raw value Marshal unexpectedly failed: %v", merr)
		}
		if !bytes.Equal(entry.GetValue(), marshaled) {
			t.Fatalf("payload not preserved: %q -> %q", marshaled, entry.GetValue())
		}

		// Fidelity: type enum preserved (stored as int32 of config.Type).
		if entry.GetType() != int32(val.Type()) {
			t.Fatalf("type not preserved: %d -> %d", int32(val.Type()), entry.GetType())
		}

		// Idempotence: converting again yields an identical payload/type.
		entry2, err2 := valueToProto(ctx, namespace, key, val)
		if err2 != nil {
			t.Fatalf("second valueToProto errored: %v", err2)
		}
		if !bytes.Equal(entry2.GetValue(), entry.GetValue()) ||
			entry2.GetType() != entry.GetType() ||
			entry2.GetCodec() != entry.GetCodec() {
			t.Fatal("valueToProto not idempotent across repeated calls")
		}
	})
}
