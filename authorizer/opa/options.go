package opa

import (
	"crypto/tls"
	"time"
)

type options struct {
	authHeader         string        // default "authorization"
	subjectClaim       string        // JWT claim used as UserID; default "sub"
	bundlePollInterval time.Duration // for NewBundleAuthorizer; default 30s
	tlsConfig          *tls.Config   // for bundle HTTP fetching
}

func defaultOptions() options {
	return options{
		authHeader:         "authorization",
		subjectClaim:       "sub",
		bundlePollInterval: 30 * time.Second,
	}
}

// Option configures an Authorizer.
type Option func(*options)

// WithAuthHeader sets the gRPC metadata key (or HTTP header) to read the
// bearer token from. Default is "authorization".
func WithAuthHeader(header string) Option {
	return func(o *options) {
		o.authHeader = header
	}
}

// WithSubjectClaim sets the JWT claim used as the user ID. Default is "sub".
func WithSubjectClaim(claim string) Option {
	return func(o *options) {
		o.subjectClaim = claim
	}
}

// WithBundlePollInterval sets how often the bundle URL is re-fetched.
// Only meaningful for NewBundleAuthorizer. Default is 30s.
func WithBundlePollInterval(d time.Duration) Option {
	return func(o *options) {
		o.bundlePollInterval = d
	}
}

// WithTLSConfig sets the TLS config for bundle HTTP fetching.
func WithTLSConfig(cfg *tls.Config) Option {
	return func(o *options) {
		o.tlsConfig = cfg
	}
}
