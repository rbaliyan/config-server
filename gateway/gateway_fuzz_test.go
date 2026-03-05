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

	f.Fuzz(func(t *testing.T, query string) {
		u, err := url.Parse("http://localhost/watch?" + query)
		if err != nil {
			return
		}
		r := &http.Request{URL: u}
		_, _ = parseWatchQuery(r)
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

	f.Fuzz(func(t *testing.T, header string) {
		_ = isForwardableHeader(header)
	})
}
