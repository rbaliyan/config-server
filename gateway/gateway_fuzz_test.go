package gateway

import (
	"net/http"
	"net/url"
	"strings"
	"testing"
)

func FuzzSanitizeSSEField(f *testing.F) {
	f.Add("simple_event")
	f.Add("event\ninjection")
	f.Add("event\r\ninjection")
	f.Add("event\rinjection")
	f.Add("")
	f.Add("\n\r\n\r")
	f.Add("no-special-chars")
	f.Add(strings.Repeat("a", 1000))
	// Adversarial: SSE field/frame injection, control chars, unicode line
	// separators, embedded NULs, oversized.
	f.Add("data: malicious\nevent: override")
	f.Add("id: 5\r\nretry: 0")
	f.Add("normal unicode-line-sep")
	f.Add("normal unicode-para-sep")
	f.Add("\x00\x01\x02control")
	f.Add("multi\nline\nfield\nvalue")
	f.Add("trailing-newline\n")
	f.Add("\nleading-newline")
	f.Add("日本語イベント")
	f.Add("emoji-🔥-field")
	f.Add(": comment-line")
	f.Add(strings.Repeat("x\n", 5000))

	f.Fuzz(func(t *testing.T, s string) {
		result := sanitizeSSEField(s)
		if strings.ContainsAny(result, "\r\n") {
			t.Errorf("sanitizeSSEField left newlines in output: %q", result)
		}
	})
}

func FuzzParseWatchQuery(f *testing.F) {
	f.Add("namespaces=prod&namespaces=staging&prefixes=app/")
	f.Add("")
	f.Add("namespaces=a")
	f.Add("prefixes=x/y/z")
	f.Add("namespaces=" + strings.Repeat("a", 300))
	// Realistic combos and edge cases.
	f.Add("namespaces=prod&prefixes=app/&prefixes=db/")
	f.Add("prefixes=")
	f.Add("namespaces=&prefixes=")
	f.Add("namespaces=a,b,c")
	f.Add("last_event_id=42")
	f.Add("namespaces=prod&last_event_id=100")
	// Adversarial: percent-encoding, injection, malformed query, unicode,
	// repeated keys, overlong, control chars.
	f.Add("namespaces=%2e%2e%2f&prefixes=%00")
	f.Add("namespaces=a%20b&prefixes=c%2Fd")
	f.Add("namespaces=prod&namespaces=" + strings.Repeat("x", 5000))
	f.Add("%ZZ=broken")
	f.Add("namespaces=日本語&prefixes=鍵")
	f.Add("namespaces=a\nb&prefixes=c\rd")
	f.Add("=novalue&novalue=")
	f.Add(strings.Repeat("namespaces=n&", 1000))
	f.Add("namespaces[]=a&namespaces[]=b")

	f.Fuzz(func(t *testing.T, query string) {
		u, err := url.Parse("http://localhost/watch?" + query)
		if err != nil {
			return
		}
		r := &http.Request{URL: u}
		_, _, _ = parseWatchQuery(r)
	})
}

func FuzzIsForwardableHeader(f *testing.F) {
	f.Add("authorization")
	f.Add("x-custom-header")
	f.Add("x-forwarded-for")
	f.Add("content-type")
	f.Add("")
	f.Add("x-real-ip")
	f.Add("x-request-id")
	f.Add("connection")
	// Realistic header names (hop-by-hop, standard, custom).
	f.Add("Authorization")
	f.Add("AUTHORIZATION")
	f.Add("cookie")
	f.Add("host")
	f.Add("keep-alive")
	f.Add("transfer-encoding")
	f.Add("upgrade")
	f.Add("te")
	f.Add("trailer")
	f.Add("proxy-authorization")
	f.Add("accept-encoding")
	// Adversarial: injection, case games, whitespace, unicode, control chars,
	// oversized.
	f.Add("x-custom\r\nx-injected")
	f.Add(" authorization ")
	f.Add("x-é-unicode")
	f.Add("x-\x00null")
	f.Add(strings.Repeat("x-", 5000))
	f.Add(":authority")

	f.Fuzz(func(t *testing.T, header string) {
		_ = isForwardableHeader(header)
	})
}
