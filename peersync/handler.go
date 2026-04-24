package peersync

import (
	"encoding/json"
	"net/http"
)

// statusResponse is the JSON body returned by the status handler.
type statusResponse struct {
	Self      Member            `json:"self"`
	Epoch     int64             `json:"epoch"`
	Members   []Member          `json:"members"`
	Overrides map[string]string `json:"overrides,omitempty"`
}

// NewStatusHandler returns an http.Handler that serves the current ring state
// as JSON. It is safe to mount at any path (e.g. /debug/ring or
// /healthz/ring). The response reflects the point-in-time state at the moment
// each request is received; it is not streamed.
//
// Example response:
//
//	{
//	  "self":    {"id": "node1", "addr": "10.0.0.1:9000"},
//	  "epoch":   7,
//	  "members": [{"id": "node1", "addr": "10.0.0.1:9000"}, ...],
//	  "overrides": {"payments": "node1"}
//	}
func NewStatusHandler(s *SyncStore) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		snap := s.Snapshot()
		resp := statusResponse{
			Self:      s.Self(),
			Epoch:     snap.Epoch,
			Members:   snap.Members,
			Overrides: snap.Overrides,
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(resp); err != nil {
			http.Error(w, "internal error", http.StatusInternalServerError)
		}
	})
}
