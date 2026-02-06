package gateway

import (
	"context"
	"crypto/tls"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/rbaliyan/config"
	"github.com/rbaliyan/config-server/service"
	"github.com/rbaliyan/config/memory"
	"google.golang.org/grpc"
)

func TestNewHandler_InvalidAddr(t *testing.T) {
	ctx := context.Background()

	_, err := NewHandler(ctx, "", WithInsecure())
	if err == nil {
		t.Fatal("expected error for empty address")
	}
}

func TestNewInProcessHandler(t *testing.T) {
	ctx := context.Background()

	store := memory.NewStore()
	if err := store.Connect(ctx); err != nil {
		t.Fatalf("failed to connect store: %v", err)
	}
	defer store.Close(ctx)

	svc := service.NewService(store, service.WithAuthorizer(service.AllowAll()))

	handler, err := NewInProcessHandler(ctx, svc)
	if err != nil {
		t.Fatalf("NewInProcessHandler failed: %v", err)
	}
	if handler == nil {
		t.Fatal("expected non-nil handler")
	}
}

func TestOptions(t *testing.T) {
	t.Run("WithInsecure", func(t *testing.T) {
		o := &options{secure: true}
		WithInsecure()(o)
		if o.secure {
			t.Error("secure should be false after WithInsecure")
		}
		if o.tlsConfig != nil {
			t.Error("tlsConfig should be nil after WithInsecure")
		}
	})

	t.Run("WithTLS_nil", func(t *testing.T) {
		o := &options{}
		WithTLS(nil)(o)
		if !o.secure {
			t.Error("secure should be true after WithTLS")
		}
		if o.tlsConfig != nil {
			t.Error("tlsConfig should be nil when WithTLS(nil)")
		}
	})

	t.Run("WithTLS_config", func(t *testing.T) {
		cfg := &tls.Config{MinVersion: tls.VersionTLS13}
		o := &options{}
		WithTLS(cfg)(o)
		if !o.secure {
			t.Error("secure should be true after WithTLS")
		}
		if o.tlsConfig != cfg {
			t.Error("tlsConfig should be the provided config")
		}
	})

	t.Run("WithDialOptions", func(t *testing.T) {
		o := &options{}
		WithDialOptions()(o) // no-op call
		if len(o.dialOpts) != 0 {
			t.Errorf("dialOpts length = %d, want 0", len(o.dialOpts))
		}
	})

	t.Run("WithMuxOptions", func(t *testing.T) {
		o := &options{}
		WithMuxOptions()(o) // no-op call
		if len(o.muxOpts) != 0 {
			t.Errorf("muxOpts length = %d, want 0", len(o.muxOpts))
		}
	})

	t.Run("buildDialOpts_insecure", func(t *testing.T) {
		o := &options{secure: false}
		opts := o.buildDialOpts()
		if len(opts) == 0 {
			t.Fatal("expected at least one dial option for credentials")
		}
	})

	t.Run("buildDialOpts_tls_default", func(t *testing.T) {
		o := &options{secure: true}
		opts := o.buildDialOpts()
		if len(opts) == 0 {
			t.Fatal("expected at least one dial option for TLS credentials")
		}
	})

	t.Run("buildDialOpts_tls_custom", func(t *testing.T) {
		cfg := &tls.Config{MinVersion: tls.VersionTLS13}
		o := &options{secure: true, tlsConfig: cfg}
		opts := o.buildDialOpts()
		if len(opts) == 0 {
			t.Fatal("expected at least one dial option for custom TLS credentials")
		}
	})

	t.Run("WithDialOptions_multiple", func(t *testing.T) {
		o := &options{}
		opt1 := grpc.WithAuthority("auth1")
		opt2 := grpc.WithAuthority("auth2")
		WithDialOptions(opt1, opt2)(o)
		if len(o.dialOpts) != 2 {
			t.Errorf("dialOpts length = %d, want 2", len(o.dialOpts))
		}
	})

	t.Run("WithMuxOptions_custom", func(t *testing.T) {
		o := &options{}
		muxOpt := runtime.WithErrorHandler(runtime.DefaultHTTPErrorHandler)
		WithMuxOptions(muxOpt)(o)
		if len(o.muxOpts) != 1 {
			t.Errorf("muxOpts length = %d, want 1", len(o.muxOpts))
		}
	})

	t.Run("buildDialOpts_with_user_opts", func(t *testing.T) {
		o := &options{
			secure:   false,
			dialOpts: []grpc.DialOption{grpc.WithAuthority("custom")},
		}
		opts := o.buildDialOpts()
		// credentials + user option
		if len(opts) < 2 {
			t.Fatalf("buildDialOpts() returned %d opts, want at least 2", len(opts))
		}
	})
}

func TestNewInProcessHandler_WithOptions(t *testing.T) {
	ctx := context.Background()

	store := memory.NewStore()
	if err := store.Connect(ctx); err != nil {
		t.Fatalf("failed to connect store: %v", err)
	}
	defer store.Close(ctx)

	svc := service.NewService(store, service.WithAuthorizer(service.AllowAll()))

	// With MuxOptions
	handler, err := NewInProcessHandler(ctx, svc,
		WithMuxOptions(runtime.WithErrorHandler(runtime.DefaultHTTPErrorHandler)),
	)
	if err != nil {
		t.Fatalf("NewInProcessHandler failed: %v", err)
	}
	if handler == nil {
		t.Fatal("expected non-nil handler")
	}
}

func TestNewInProcessHandler_ServeHTTP(t *testing.T) {
	ctx := context.Background()

	store := memory.NewStore()
	if err := store.Connect(ctx); err != nil {
		t.Fatalf("failed to connect store: %v", err)
	}
	defer store.Close(ctx)

	// Seed test data
	if _, err := store.Set(ctx, "test", "key1", config.NewValue("val1")); err != nil {
		t.Fatalf("failed to seed test data: %v", err)
	}

	svc := service.NewService(store, service.WithAuthorizer(service.AllowAll()))

	handler, err := NewInProcessHandler(ctx, svc)
	if err != nil {
		t.Fatalf("NewInProcessHandler failed: %v", err)
	}

	// Test GET /v1/namespaces/test/keys/key1
	req := httptest.NewRequest(http.MethodGet, "/v1/namespaces/test/keys/key1", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("GET status = %d, want 200; body: %s", rec.Code, rec.Body.String())
	}
}

func TestNewInProcessHandler_NotFound(t *testing.T) {
	ctx := context.Background()

	store := memory.NewStore()
	if err := store.Connect(ctx); err != nil {
		t.Fatalf("failed to connect store: %v", err)
	}
	defer store.Close(ctx)

	svc := service.NewService(store, service.WithAuthorizer(service.AllowAll()))

	handler, err := NewInProcessHandler(ctx, svc)
	if err != nil {
		t.Fatalf("NewInProcessHandler failed: %v", err)
	}

	// Non-existent key
	req := httptest.NewRequest(http.MethodGet, "/v1/namespaces/test/keys/nonexistent", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Errorf("GET nonexistent status = %d, want 404; body: %s", rec.Code, rec.Body.String())
	}
}

func TestNewInProcessHandler_InvalidRoute(t *testing.T) {
	ctx := context.Background()

	store := memory.NewStore()
	if err := store.Connect(ctx); err != nil {
		t.Fatalf("failed to connect store: %v", err)
	}
	defer store.Close(ctx)

	svc := service.NewService(store, service.WithAuthorizer(service.AllowAll()))

	handler, err := NewInProcessHandler(ctx, svc)
	if err != nil {
		t.Fatalf("NewInProcessHandler failed: %v", err)
	}

	// Invalid route
	req := httptest.NewRequest(http.MethodGet, "/v1/nonexistent", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	// Should return 404 or 405, not 200
	if rec.Code == http.StatusOK {
		t.Errorf("expected non-200 status for invalid route, got %d", rec.Code)
	}
}

func TestNewHandler_WithInsecure(t *testing.T) {
	ctx := context.Background()

	// This creates a handler pointing to a non-existent address.
	// It should still succeed since gRPC uses lazy connection.
	handler, err := NewHandler(ctx, "localhost:19999", WithInsecure())
	if err != nil {
		t.Fatalf("NewHandler failed: %v", err)
	}
	if handler == nil {
		t.Fatal("expected non-nil handler")
	}
}

func TestNewHandler_WithTLS(t *testing.T) {
	ctx := context.Background()

	handler, err := NewHandler(ctx, "localhost:19999", WithTLS(nil))
	if err != nil {
		t.Fatalf("NewHandler with TLS failed: %v", err)
	}
	if handler == nil {
		t.Fatal("expected non-nil handler")
	}
}

func TestNewHandler_WithCustomTLS(t *testing.T) {
	ctx := context.Background()

	cfg := &tls.Config{MinVersion: tls.VersionTLS13}
	handler, err := NewHandler(ctx, "localhost:19999", WithTLS(cfg))
	if err != nil {
		t.Fatalf("NewHandler with custom TLS failed: %v", err)
	}
	if handler == nil {
		t.Fatal("expected non-nil handler")
	}
}

func TestNewHandler_WithDialOptions(t *testing.T) {
	ctx := context.Background()

	handler, err := NewHandler(ctx, "localhost:19999",
		WithInsecure(),
		WithDialOptions(grpc.WithAuthority("custom-authority")),
	)
	if err != nil {
		t.Fatalf("NewHandler with dial options failed: %v", err)
	}
	if handler == nil {
		t.Fatal("expected non-nil handler")
	}
}

func TestNewHandler_WithMuxOptions(t *testing.T) {
	ctx := context.Background()

	handler, err := NewHandler(ctx, "localhost:19999",
		WithInsecure(),
		WithMuxOptions(runtime.WithErrorHandler(runtime.DefaultHTTPErrorHandler)),
	)
	if err != nil {
		t.Fatalf("NewHandler with mux options failed: %v", err)
	}
	if handler == nil {
		t.Fatal("expected non-nil handler")
	}
}
