package gateway

import (
	"strconv"
	"testing"

	configpb "github.com/rbaliyan/config-server/proto/config/v1"
)

const benchBufferSize = 1024

func benchResponse(key string) *configpb.WatchResponse {
	return &configpb.WatchResponse{
		Type: configpb.ChangeType_CHANGE_TYPE_SET,
		Entry: &configpb.Entry{
			Namespace: "production",
			Key:       key,
			Value:     []byte("some-config-value"),
		},
	}
}

// BenchmarkEventBufferPush measures a single ring-buffer push under lock:
// a seq increment, a strconv.FormatInt for the event id, and an in-place
// slot write. The buffer is pre-filled so every iteration exercises the
// wrap-around (eviction) branch rather than the initial fill branch.
func BenchmarkEventBufferPush(b *testing.B) {
	buf := newEventBuffer(benchBufferSize)
	resp := benchResponse("app/db")
	// Fill the ring so push always hits the eviction path.
	for i := 0; i < benchBufferSize; i++ {
		buf.push(resp)
	}

	b.ReportAllocs()
	for b.Loop() {
		buf.push(resp)
	}
}

// BenchmarkEventBufferSince measures the Last-Event-ID replay scan over a full
// buffer. since walks all b.len entries, parsing each id, and allocates a
// result slice — an O(buffer) cost paid on every SSE reconnect. The lastEventID
// is chosen to return roughly half the buffer, a realistic mid-buffer resume.
func BenchmarkEventBufferSince(b *testing.B) {
	buf := newEventBuffer(benchBufferSize)
	resp := benchResponse("app/db")
	for i := 0; i < benchBufferSize; i++ {
		buf.push(resp)
	}
	// Resume from the middle of the buffer's id range.
	lastEventID := strconv.Itoa(benchBufferSize / 2)

	b.ReportAllocs()
	for b.Loop() {
		_ = buf.since(lastEventID)
	}
}
