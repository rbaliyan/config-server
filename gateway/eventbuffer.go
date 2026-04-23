package gateway

import (
	"strconv"
	"sync"

	configpb "github.com/rbaliyan/config-server/proto/config/v1"
)

type bufferedEvent struct {
	id   string
	resp *configpb.WatchResponse
}

type eventBuffer struct {
	mu   sync.Mutex
	ring []bufferedEvent
	size int
	head int
	len  int
	seq  int64
}

func newEventBuffer(size int) *eventBuffer {
	if size <= 0 {
		return &eventBuffer{}
	}
	return &eventBuffer{
		ring: make([]bufferedEvent, size),
		size: size,
	}
}

func (b *eventBuffer) push(resp *configpb.WatchResponse) string {
	if b.size == 0 {
		return ""
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.seq++
	id := strconv.FormatInt(b.seq, 10)
	idx := (b.head + b.len) % b.size
	if b.len == b.size {
		b.ring[b.head] = bufferedEvent{id: id, resp: resp}
		b.head = (b.head + 1) % b.size
	} else {
		b.ring[idx] = bufferedEvent{id: id, resp: resp}
		b.len++
	}
	return id
}

// since returns buffered events with an integer id greater than lastEventID.
// If lastEventID is empty or malformed, nothing is returned — replaying the
// entire buffer is a surprising default that would race against the live
// stream for clients that didn't explicitly request resumption.
func (b *eventBuffer) since(lastEventID string) []bufferedEvent {
	if b.size == 0 || lastEventID == "" {
		return nil
	}
	lastID, err := strconv.ParseInt(lastEventID, 10, 64)
	if err != nil {
		return nil
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.len == 0 {
		return nil
	}
	var result []bufferedEvent
	for i := 0; i < b.len; i++ {
		ev := b.ring[(b.head+i)%b.size]
		evID, err := strconv.ParseInt(ev.id, 10, 64)
		if err != nil {
			continue
		}
		if evID > lastID {
			result = append(result, ev)
		}
	}
	return result
}
