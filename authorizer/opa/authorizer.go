// Package opa provides an OPA (Open Policy Agent) implementation of
// service.SecurityGuard for config-server. It turns every authorization
// decision into a Rego policy evaluation and extracts the caller's identity
// from gRPC metadata (by default the "authorization" header, with the
// "Bearer " prefix stripped).
//
// # JWT handling and its security implications
//
// If the incoming credential looks like a JWT, the authorizer base64-decodes
// the claims segment and makes it available to the policy as
// input.identity.claims. The signature, issuer, audience, expiry, and any
// other standard JWT validations are NOT performed by this package.
//
// This means that, out of the box, any client can forge claims by handing
// over any well-formed but unsigned JWT. Before deploying this guard in
// production you MUST do one of the following:
//
//   - Perform signature verification upstream (an API gateway, ingress proxy,
//     or service mesh) and trust only traffic from that component.
//   - Call OPA's built-in token introspection functions (io.jwt.verify_*,
//     io.jwt.decode_verify) from the Rego policy to validate the token
//     before returning allow=true.
//
// # Construction
//
// NewAuthorizer compiles inline Rego source. NewBundleAuthorizer fetches the
// policy from an HTTP(S) URL at construction time and re-fetches it at the
// configured poll interval (see WithBundlePollInterval).
//
// # Usage
//
//	guard, err := opa.NewAuthorizer(ctx, policySrc, "data.config.authz.allow")
//	if err != nil { return err }
//	defer guard.Close()
//
//	svc, _ := service.NewService(store, service.WithSecurityGuard(guard))
package opa

import (
	"context"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/open-policy-agent/opa/v1/rego"
	"github.com/rbaliyan/config-server/service"
	"google.golang.org/grpc/metadata"
)

// Authorizer implements service.SecurityGuard using OPA policy evaluation.
type Authorizer struct {
	query     atomic.Pointer[rego.PreparedEvalQuery]
	opts      options
	stopCh    chan struct{}
	closeOnce sync.Once
	wg        sync.WaitGroup
}

var _ service.SecurityGuard = (*Authorizer)(nil)

// NewAuthorizer creates an Authorizer from inline Rego policy source.
// policy is the full Rego module text and entrypoint is the rule path
// (e.g. "data.config.authz.allow").
//
// Example policy:
//
//	package config.authz
//	default allow = false
//	allow if {
//	    input.action == "read"
//	    input.identity.user_id != ""
//	}
func NewAuthorizer(ctx context.Context, policy, entrypoint string, opts ...Option) (*Authorizer, error) {
	if policy == "" {
		return nil, fmt.Errorf("opa: policy must not be empty")
	}
	if entrypoint == "" {
		return nil, fmt.Errorf("opa: entrypoint must not be empty")
	}

	o := defaultOptions()
	for _, opt := range opts {
		opt(&o)
	}

	q, err := prepareQuery(ctx, policy, entrypoint)
	if err != nil {
		return nil, err
	}

	a := &Authorizer{opts: o, stopCh: make(chan struct{})}
	a.query.Store(q)
	return a, nil
}

// NewBundleAuthorizer creates an Authorizer that loads policy from an OPA
// bundle URL. The bundle is fetched at construction and re-fetched at the
// configured poll interval.
func NewBundleAuthorizer(ctx context.Context, bundleURL, entrypoint string, opts ...Option) (*Authorizer, error) {
	if bundleURL == "" {
		return nil, fmt.Errorf("opa: bundleURL must not be empty")
	}
	if entrypoint == "" {
		return nil, fmt.Errorf("opa: entrypoint must not be empty")
	}

	o := defaultOptions()
	for _, opt := range opts {
		opt(&o)
	}

	// Initial load
	policy, err := fetchBundle(ctx, bundleURL, o.tlsConfig)
	if err != nil {
		return nil, fmt.Errorf("opa: initial bundle fetch: %w", err)
	}

	q, err := prepareQuery(ctx, policy, entrypoint)
	if err != nil {
		return nil, err
	}

	a := &Authorizer{opts: o, stopCh: make(chan struct{})}
	a.query.Store(q)

	// Background poller. Use a fresh context derived from Background and
	// cancelled via stopCh so the lifecycle of the poller is controlled by
	// Close(), independent of the constructor-scoped ctx (which commonly
	// expires right after NewBundleAuthorizer returns).
	a.wg.Add(1)
	go func() {
		defer a.wg.Done()

		pollCtx, cancel := context.WithCancel(context.Background())
		defer cancel()
		// Translate stopCh close into ctx cancellation so in-flight HTTP
		// requests unblock immediately.
		go func() {
			select {
			case <-a.stopCh:
				cancel()
			case <-pollCtx.Done():
			}
		}()

		ticker := time.NewTicker(o.bundlePollInterval)
		defer ticker.Stop()
		for {
			select {
			case <-a.stopCh:
				return
			case <-ticker.C:
				p, err := fetchBundle(pollCtx, bundleURL, o.tlsConfig)
				if err != nil {
					slog.Default().Error("opa: bundle refresh failed", "error", err)
					continue
				}
				nq, err := prepareQuery(pollCtx, p, entrypoint)
				if err != nil {
					slog.Default().Error("opa: re-prepare query failed", "error", err)
					continue
				}
				a.query.Store(nq)
			}
		}
	}()

	return a, nil
}

// Close stops any background goroutines started by NewBundleAuthorizer.
// Safe to call multiple times; subsequent calls are no-ops.
func (a *Authorizer) Close() error {
	a.closeOnce.Do(func() {
		close(a.stopCh)
	})
	a.wg.Wait()
	return nil
}

// Authenticate extracts the caller's identity from gRPC incoming metadata.
// It reads the configured auth header, strips a "Bearer " prefix, and
// attempts to parse the token as a JWT to extract claims. If the token is not
// a valid JWT it is used as-is for the user ID. Missing tokens yield an
// anonymous identity.
func (a *Authorizer) Authenticate(ctx context.Context) (service.Identity, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return &opaIdentity{userID: "anonymous", claims: map[string]any{}}, nil
	}

	values := md.Get(a.opts.authHeader)
	if len(values) == 0 {
		return &opaIdentity{userID: "anonymous", claims: map[string]any{}}, nil
	}

	token := strings.TrimPrefix(values[0], "Bearer ")
	token = strings.TrimPrefix(token, "bearer ")

	// Parse JWT claims (no signature verification — OPA policy handles authz).
	claims, err := parseJWTClaims(token)
	if err != nil {
		// Not a JWT; treat as opaque token, use it as the user ID.
		return &opaIdentity{userID: token, claims: map[string]any{}}, nil
	}

	userID, _ := claims[a.opts.subjectClaim].(string)
	if userID == "" {
		userID = "anonymous"
	}
	return &opaIdentity{userID: userID, claims: claims}, nil
}

// Authorize evaluates the configured OPA policy with the provided identity,
// action, and resource as input.
func (a *Authorizer) Authorize(ctx context.Context, id service.Identity, action string, resource service.Resource) (service.Decision, error) {
	input := map[string]any{
		"action": action,
		"resource": map[string]any{
			"namespace": resource.Namespace,
			"key":       resource.Key,
		},
		"identity": map[string]any{
			"user_id": id.UserID(),
			"claims":  id.Claims(),
		},
	}

	q := a.query.Load()
	if q == nil {
		return service.Decision{Allowed: false, Reason: "OPA query not initialized"}, nil
	}

	rs, err := q.Eval(ctx, rego.EvalInput(input))
	if err != nil {
		return service.Decision{}, fmt.Errorf("opa: eval: %w", err)
	}

	if len(rs) == 0 || len(rs[0].Expressions) == 0 {
		return service.Decision{Allowed: false, Reason: "no OPA decision"}, nil
	}

	allowed, _ := rs[0].Expressions[0].Value.(bool)
	reason := ""
	if !allowed {
		reason = "OPA policy denied"
	}
	return service.Decision{Allowed: allowed, Reason: reason}, nil
}

// opaIdentity holds the authenticated identity derived from a token.
type opaIdentity struct {
	userID string
	claims map[string]any
}

func (i *opaIdentity) UserID() string         { return i.userID }
func (i *opaIdentity) Claims() map[string]any { return i.claims }

// prepareQuery compiles a Rego policy and prepares it for evaluation.
func prepareQuery(ctx context.Context, policy, entrypoint string) (*rego.PreparedEvalQuery, error) {
	r := rego.New(
		rego.Query(entrypoint),
		rego.Module("policy.rego", policy),
	)
	q, err := r.PrepareForEval(ctx)
	if err != nil {
		return nil, fmt.Errorf("opa: prepare query: %w", err)
	}
	return &q, nil
}

// fetchBundle fetches a policy bundle (raw Rego source) from a URL.
// An optional TLS config may be provided for HTTPS endpoints.
func fetchBundle(ctx context.Context, url string, tlsCfg *tls.Config) (string, error) {
	transport := http.DefaultTransport
	if tlsCfg != nil {
		transport = &http.Transport{TLSClientConfig: tlsCfg}
	}
	client := &http.Client{Transport: transport}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil) // #nosec G107 -- URL is operator-supplied configuration, not user input
	if err != nil {
		return "", fmt.Errorf("build request: %w", err)
	}

	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("http get: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("unexpected status %d from bundle URL", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("read body: %w", err)
	}
	return string(body), nil
}

// parseJWTClaims parses the claims from a JWT without verifying the signature.
// Returns an error if the token is not in valid JWT format.
func parseJWTClaims(token string) (map[string]any, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return nil, fmt.Errorf("not a JWT")
	}
	// Decode the claims part (second segment) using RawURLEncoding (no padding).
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return nil, fmt.Errorf("decode JWT claims: %w", err)
	}
	var claims map[string]any
	if err := json.Unmarshal(payload, &claims); err != nil {
		return nil, fmt.Errorf("unmarshal JWT claims: %w", err)
	}
	return claims, nil
}
