package gateway

import (
	"context"
	"crypto/tls"
	"testing"

	"github.com/rbaliyan/config-server/service"
	"github.com/rbaliyan/config/memory"
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
}
