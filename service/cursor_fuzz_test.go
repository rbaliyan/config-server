package service

import "testing"

// FuzzDecodeNamespaceCursor fuzzes the attacker-controlled ListNamespaces
// pagination cursor (base64-url). It must never panic, and any cursor that
// decodes must round-trip: re-encoding the decoded name yields a cursor that
// decodes back to the same name.
func FuzzDecodeNamespaceCursor(f *testing.F) {
	f.Add("")
	f.Add("cHJvZA==")         // "prod"
	f.Add("YQ==")             // "a"
	f.Add("not!base64")       // invalid alphabet
	f.Add("////")             // wrong (std, not url) alphabet padding
	f.Add("AAAAAAAAAAAA")     // long zero run
	f.Add("cHJvZC91cy93ZXN0") // "prod/us/west"

	f.Fuzz(func(t *testing.T, cursor string) {
		name, err := decodeNamespaceCursor(cursor)
		if err != nil {
			return // malformed cursor rejected cleanly — acceptable
		}
		reenc := encodeNamespaceCursor(name)
		back, err := decodeNamespaceCursor(reenc)
		if err != nil {
			t.Fatalf("re-encoded cursor failed to decode: name=%q reenc=%q err=%v", name, reenc, err)
		}
		if back != name {
			t.Fatalf("cursor round-trip mismatch: %q -> %q -> %q", cursor, name, back)
		}
	})
}
