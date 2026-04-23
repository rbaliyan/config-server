package gateway

import (
	"strconv"
	"sync"
	"testing"

	configpb "github.com/rbaliyan/config-server/proto/config/v1"
)

func mkResp(i int) *configpb.WatchResponse {
	return &configpb.WatchResponse{
		Type: configpb.ChangeType_CHANGE_TYPE_SET,
		Entry: &configpb.Entry{
			Namespace: "ns",
			Key:       "k" + strconv.Itoa(i),
		},
	}
}

func TestEventBuffer_DisabledWhenSizeZero(t *testing.T) {
	b := newEventBuffer(0)
	if id := b.push(mkResp(1)); id != "" {
		t.Fatalf("expected empty id when size=0, got %q", id)
	}
	if got := b.since(""); got != nil {
		t.Fatalf("expected nil from since when size=0, got %v", got)
	}
	if got := b.since("1"); got != nil {
		t.Fatalf("expected nil from since when size=0 even with id, got %v", got)
	}
}

func TestEventBuffer_IDsMonotonicAndOrdered(t *testing.T) {
	b := newEventBuffer(3)
	id1 := b.push(mkResp(1))
	id2 := b.push(mkResp(2))
	id3 := b.push(mkResp(3))
	if id1 >= id2 || id2 >= id3 {
		t.Fatalf("IDs must be monotonically increasing: %q %q %q", id1, id2, id3)
	}
}

func TestEventBuffer_Since(t *testing.T) {
	b := newEventBuffer(5)
	var ids []string
	for i := 1; i <= 5; i++ {
		ids = append(ids, b.push(mkResp(i)))
	}

	// Empty lastEventID: buffer is not replayed (safety default).
	if got := b.since(""); got != nil {
		t.Fatalf("expected nil with empty lastEventID, got %d items", len(got))
	}
	// Malformed ID: nothing returned.
	if got := b.since("abc"); got != nil {
		t.Fatalf("expected nil with malformed lastEventID, got %d items", len(got))
	}
	// All newer.
	if got := b.since("0"); len(got) != 5 {
		t.Fatalf("expected 5 items from since(\"0\"), got %d", len(got))
	}
	// From the middle.
	got := b.since(ids[2])
	if len(got) != 2 {
		t.Fatalf("expected 2 items after ids[2], got %d", len(got))
	}
	if got[0].id != ids[3] || got[1].id != ids[4] {
		t.Fatalf("ids do not match expected: got %q, %q", got[0].id, got[1].id)
	}
}

func TestEventBuffer_RingEvictsOldest(t *testing.T) {
	b := newEventBuffer(3)
	b.push(mkResp(1))
	b.push(mkResp(2))
	b.push(mkResp(3))
	b.push(mkResp(4)) // should evict 1
	b.push(mkResp(5)) // should evict 2

	got := b.since("0")
	if len(got) != 3 {
		t.Fatalf("expected 3 items after eviction, got %d", len(got))
	}
	// Remaining should be 3,4,5 in order.
	for i, ev := range got {
		entry := ev.resp.GetEntry()
		expectedKey := "k" + strconv.Itoa(i+3)
		if entry.GetKey() != expectedKey {
			t.Fatalf("pos %d: expected %q, got %q", i, expectedKey, entry.GetKey())
		}
	}
}

func TestEventBuffer_ConcurrentPushAndSince(t *testing.T) {
	b := newEventBuffer(32)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 500; i++ {
			b.push(mkResp(i))
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 500; i++ {
			_ = b.since("10")
		}
	}()
	wg.Wait()
}

func TestResolveEventBufferSize(t *testing.T) {
	if got := resolveEventBufferSize(nil); got != defaultEventBufferSize {
		t.Fatalf("nil should yield default, got %d", got)
	}
	zero := 0
	if got := resolveEventBufferSize(&zero); got != 0 {
		t.Fatalf("explicit 0 must be respected, got %d", got)
	}
	seven := 7
	if got := resolveEventBufferSize(&seven); got != 7 {
		t.Fatalf("expected 7, got %d", got)
	}
}
