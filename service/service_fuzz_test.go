package service

import (
	"context"
	"testing"

	"github.com/rbaliyan/config"
	"github.com/rbaliyan/config/memory"
	configpb "github.com/rbaliyan/config-server/proto/config/v1"
)

func FuzzServiceGet(f *testing.F) {
	f.Add("production", "app/database/host")
	f.Add("", "")
	f.Add("ns", "key")
	f.Add("special!ns", "key/../traversal")
	f.Add("prod", "/leading-slash")

	store := memory.NewStore()
	svc, err := NewService(store, WithAuthorizer(AllowAll()))
	if err != nil {
		f.Fatal(err)
	}

	f.Fuzz(func(t *testing.T, namespace, key string) {
		_, _ = svc.Get(context.Background(), &configpb.GetRequest{
			Namespace: namespace,
			Key:       key,
		})
	})
}

func FuzzServiceSet(f *testing.F) {
	f.Add("production", "app/key", []byte(`"hello"`), "json")
	f.Add("", "", []byte{}, "")
	f.Add("ns", "key", []byte(`{"nested":true}`), "json")
	f.Add("ns", "key", []byte(`invalid json`), "json")
	f.Add("ns", "key", []byte(`value: true`), "yaml")

	store := memory.NewStore()
	svc, err := NewService(store, WithAuthorizer(AllowAll()))
	if err != nil {
		f.Fatal(err)
	}

	f.Fuzz(func(t *testing.T, namespace, key string, value []byte, codec string) {
		_, _ = svc.Set(context.Background(), &configpb.SetRequest{
			Namespace: namespace,
			Key:       key,
			Value:     value,
			Codec:     codec,
		})
	})
}

func FuzzServiceList(f *testing.F) {
	f.Add("production", "app/", int32(100), "")
	f.Add("", "", int32(0), "")
	f.Add("ns", "prefix", int32(-1), "cursor123")

	store := memory.NewStore()
	svc, err := NewService(store, WithAuthorizer(AllowAll()))
	if err != nil {
		f.Fatal(err)
	}

	// Pre-populate some data
	_, _ = store.Set(context.Background(), "production", "app/db", config.NewValue("localhost"))
	_, _ = store.Set(context.Background(), "production", "app/port", config.NewValue(5432))

	f.Fuzz(func(t *testing.T, namespace, prefix string, limit int32, cursor string) {
		_, _ = svc.List(context.Background(), &configpb.ListRequest{
			Namespace: namespace,
			Prefix:    prefix,
			Limit:     limit,
			Cursor:    cursor,
		})
	})
}
