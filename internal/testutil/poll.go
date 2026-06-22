// Package testutil provides shared helpers for the config-server test suites.
package testutil

import (
	"testing"
	"time"
)

// Eventually polls cond until it returns true or timeout elapses, checking
// every interval. It returns true if cond became true within the deadline,
// false otherwise. It is a bounded replacement for fixed time.Sleep waits in
// integration tests: a fast condition returns immediately, while a wedged
// dependency fails the poll deterministically instead of hanging.
//
// interval values <= 0 default to 5ms.
func Eventually(timeout, interval time.Duration, cond func() bool) bool {
	if interval <= 0 {
		interval = 5 * time.Millisecond
	}
	deadline := time.Now().Add(timeout)
	for {
		if cond() {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(interval)
	}
}

// WaitFor is Eventually with a t.Fatalf on timeout, carrying msg for context.
// Use it when the condition is a precondition the rest of the test depends on.
func WaitFor(t *testing.T, timeout, interval time.Duration, cond func() bool, msg string) {
	t.Helper()
	if !Eventually(timeout, interval, cond) {
		t.Fatalf("condition not met within %s: %s", timeout, msg)
	}
}
