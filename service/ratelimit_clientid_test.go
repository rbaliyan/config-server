package service

import (
	"context"
	"net"
	"testing"

	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
)

// xffLimiter is a RateLimiter that also implements ClientIdentifier by reading
// the X-Forwarded-For metadata header, falling back to the peer address. It
// models a real proxy-aware identifier so extractClientID's ClientIdentifier
// branch is exercised end to end.
type xffLimiter struct{}

func (xffLimiter) Allow(string) bool { return true }

func (xffLimiter) ClientID(ctx context.Context) string {
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if vals := md.Get("x-forwarded-for"); len(vals) > 0 && vals[0] != "" {
			return vals[0]
		}
	}
	if p, ok := peer.FromContext(ctx); ok {
		return p.Addr.String()
	}
	return "unknown"
}

// peerOnlyLimiter does NOT implement ClientIdentifier, so extractClientID must
// fall back to the peer address (or "unknown").
type peerOnlyLimiter struct{}

func (peerOnlyLimiter) Allow(string) bool { return true }

func ctxWithPeer() context.Context {
	return peer.NewContext(context.Background(), &peer.Peer{
		Addr: &net.TCPAddr{IP: net.ParseIP("10.0.0.7"), Port: 5555},
	})
}

func TestExtractClientID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		limiter RateLimiter
		ctx     func() context.Context
		want    string
	}{
		{
			name:    "ClientIdentifier reads X-Forwarded-For",
			limiter: xffLimiter{},
			ctx: func() context.Context {
				md := metadata.Pairs("x-forwarded-for", "203.0.113.9")
				return metadata.NewIncomingContext(context.Background(), md)
			},
			want: "203.0.113.9",
		},
		{
			name:    "ClientIdentifier falls back to peer when XFF absent",
			limiter: xffLimiter{},
			ctx:     func() context.Context { return ctxWithPeer() },
			want:    "10.0.0.7:5555",
		},
		{
			name:    "no ClientIdentifier uses peer address",
			limiter: peerOnlyLimiter{},
			ctx:     func() context.Context { return ctxWithPeer() },
			want:    "10.0.0.7:5555",
		},
		{
			name:    "no ClientIdentifier and no peer yields unknown",
			limiter: peerOnlyLimiter{},
			ctx:     func() context.Context { return context.Background() },
			want:    "unknown",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := extractClientID(tt.ctx(), tt.limiter)
			if got != tt.want {
				t.Errorf("extractClientID() = %q, want %q", got, tt.want)
			}
		})
	}
}
