package gateway

import (
	"strconv"
	"testing"

	configpb "github.com/rbaliyan/config-server/proto/config/v1"
)

// fuzzResp builds a minimal WatchResponse. It is defined locally (rather than
// reusing the mkResp helper in eventbuffer_test.go) so this fuzz target is
// self-contained: ClusterFuzzLite compiles the fuzz file plus production code
// only, not sibling _test.go helpers.
func fuzzResp(i int) *configpb.WatchResponse {
	return &configpb.WatchResponse{
		Type: configpb.ChangeType_CHANGE_TYPE_SET,
		Entry: &configpb.Entry{
			Namespace: "ns",
			Key:       "k" + strconv.Itoa(i),
		},
	}
}

// FuzzEventBufferSince fuzzes the SSE Last-Event-ID replay path: a fuzzed
// sequence of pushes builds the ring buffer, then a fuzzed (attacker-supplied)
// Last-Event-ID header value drives since(). The Last-Event-ID is parsed
// verbatim from an untrusted HTTP header (gateway/sse.go parseWatchQuery), so
// the parse + lookup must never panic and must never leak events outside the
// buffer or out of order.
func FuzzEventBufferSince(f *testing.F) {
	// Seeds: (buffer size, number of pushes, raw Last-Event-ID).
	f.Add(8, 5, "0")
	f.Add(8, 5, "3")
	f.Add(4, 10, "7") // wrap-around: pushes exceed size
	f.Add(0, 3, "1")  // disabled buffer
	f.Add(8, 0, "1")  // empty buffer
	f.Add(8, 5, "")   // empty header -> no replay
	f.Add(8, 5, "abc")
	f.Add(8, 5, "-1")
	f.Add(8, 5, "99999999999999999999999999") // overflow ParseInt
	f.Add(8, 5, "  5  ")
	f.Add(8, 5, "5\n6")
	f.Add(8, 5, "0x10")
	f.Add(1, 100, "50")
	// More adversarial Last-Event-ID values and buffer geometries.
	f.Add(16, 16, "0")                  // exactly full, replay all
	f.Add(16, 16, "15")                 // newest id, no replay
	f.Add(8, 5, "+3")                   // signed
	f.Add(8, 5, " 3")                   // leading space
	f.Add(8, 5, "3.0")                  // float-looking
	f.Add(8, 5, "9223372036854775807")  // int64 max
	f.Add(8, 5, "-9223372036854775808") // int64 min
	f.Add(8, 5, "日本語")                  // unicode
	f.Add(8, 5, "\x00\x01")             // control chars
	f.Add(1024, 4096, "2000")           // large wrap-around
	f.Add(2, 1, "0")                    // partial fill

	f.Fuzz(func(t *testing.T, size, pushes int, lastEventID string) {
		// Clamp fuzz inputs to sane bounds so the harness stays fast and does
		// not OOM on a giant allocation; the logic under test is unchanged.
		if size < 0 {
			size = 0
		}
		if size > 1024 {
			size = 1024
		}
		if pushes < 0 {
			pushes = 0
		}
		if pushes > 4096 {
			pushes = 4096
		}

		b := newEventBuffer(size)

		// Record the ids returned by push so we know the legal id universe and
		// the exact set currently retained in the ring.
		var pushedIDs []string
		for i := 0; i < pushes; i++ {
			id := b.push(fuzzResp(i))
			if size == 0 {
				if id != "" {
					t.Fatalf("disabled buffer returned non-empty id %q", id)
				}
				continue
			}
			if id == "" {
				t.Fatalf("enabled buffer returned empty id on push %d", i)
			}
			pushedIDs = append(pushedIDs, id)
		}

		// Invariant: since never panics on an arbitrary header value.
		got := b.since(lastEventID)

		// Disabled or empty/malformed header => no replay.
		if size == 0 {
			if got != nil {
				t.Fatalf("disabled buffer replayed %d events", len(got))
			}
			return
		}

		// Parse the header the same way since does; if it is not a valid int,
		// since must return nothing.
		parsed, perr := strconv.ParseInt(lastEventID, 10, 64)
		if perr != nil {
			if got != nil {
				t.Fatalf("malformed lastEventID %q replayed %d events", lastEventID, len(got))
			}
			return
		}

		// Build the set of ids currently retained in the ring (the most recent
		// min(pushes, size) ids).
		retained := pushedIDs
		if len(retained) > size {
			retained = retained[len(retained)-size:]
		}
		retainedSet := make(map[string]struct{}, len(retained))
		for _, id := range retained {
			retainedSet[id] = struct{}{}
		}

		var prev int64 = -1 << 62
		for _, ev := range got {
			// Invariant: every replayed event currently lives in the buffer —
			// since must never resurrect an evicted event.
			if _, ok := retainedSet[ev.id]; !ok {
				t.Fatalf("since returned id %q not retained in buffer (retained=%v)", ev.id, retained)
			}

			evID, err := strconv.ParseInt(ev.id, 10, 64)
			if err != nil {
				t.Fatalf("buffered event has non-integer id %q", ev.id)
			}

			// Invariant: only events strictly newer than lastEventID.
			if evID <= parsed {
				t.Fatalf("since(%q) returned id %d <= cutoff %d", lastEventID, evID, parsed)
			}

			// Invariant: returned in ascending id order (oldest-to-newest),
			// matching push order, so a resuming client never sees reordering.
			if evID <= prev {
				t.Fatalf("since returned ids out of order: %d after %d", evID, prev)
			}
			prev = evID

			// Sanity: the response payload survived the round trip.
			if ev.resp == nil {
				t.Fatalf("buffered event %q has nil response", ev.id)
			}
		}

		// Invariant: result count never exceeds what the buffer holds.
		if len(got) > len(retained) {
			t.Fatalf("since returned %d events but buffer holds only %d", len(got), len(retained))
		}
	})
}
