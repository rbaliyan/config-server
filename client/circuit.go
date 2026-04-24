package client

import "time"

// isCircuitOpen reports whether the circuit breaker is currently blocking
// operations. Returns false when the circuit has never been enabled, when it
// is not tripped, or when the open interval has elapsed (in which case the
// circuit is reset to allow a half-open probe).
func (s *RemoteStore) isCircuitOpen() bool {
	if !s.opts.enableCircuit {
		return false
	}

	s.circuitMu.Lock()
	defer s.circuitMu.Unlock()

	if !s.circuitOpen {
		return false
	}

	if time.Since(s.circuitOpenAt) > s.opts.circuitTimeout {
		s.circuitOpen = false
		return false
	}

	return true
}

// recordSuccess clears the consecutive-failure counter after a successful RPC.
func (s *RemoteStore) recordSuccess() {
	if !s.opts.enableCircuit {
		return
	}
	s.circuitMu.Lock()
	s.consecutiveFail = 0
	s.circuitMu.Unlock()
}

// recordFailure increments the failure counter; when it crosses the threshold
// the circuit trips and subsequent calls short-circuit until the timeout
// elapses.
func (s *RemoteStore) recordFailure() {
	if !s.opts.enableCircuit {
		return
	}
	s.circuitMu.Lock()
	s.consecutiveFail++
	if s.consecutiveFail >= s.opts.circuitThreshold {
		s.circuitOpen = true
		s.circuitOpenAt = time.Now()
	}
	s.circuitMu.Unlock()
}

// resetCircuit forces the circuit back to closed state. Called after a fresh
// Connect so a pre-existing trip does not block the first operation.
func (s *RemoteStore) resetCircuit() {
	s.circuitMu.Lock()
	s.circuitOpen = false
	s.consecutiveFail = 0
	s.circuitMu.Unlock()
}
