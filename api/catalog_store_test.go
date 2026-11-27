package api

import (
	"context"
	"net/http"
	"testing"

	"github.com/r-hashi01/kairo/blob"
	"github.com/r-hashi01/kairo/engine"
)

// With a Catalog, a plan registered through the API is kept there too: a
// daemon with a new (or lost) data directory loads it from the store, and
// can run it (ADR 0087).
func TestCatalogStoreOutlivesTheDataDirectory(t *testing.T) {
	store := blob.NewMem()
	e1 := newRegistryEngine(t)
	if err := LoadWith(e1, t.TempDir(), store); err != nil {
		t.Fatal(err)
	}
	h := (&API{E: e1, Dir: t.TempDir(), Catalog: store}).Handler()
	if w := postPlanDefinition(t, h, "kept", "w", "go"); w.Code != http.StatusOK && w.Code != http.StatusCreated {
		t.Fatalf("register: %d %s", w.Code, w.Body)
	}

	e2 := newRegistryEngine(t)
	if err := LoadWith(e2, t.TempDir(), store); err != nil {
		t.Fatal(err)
	}
	if err := e2.Start(); err != nil {
		t.Fatal(err)
	}
	if _, err := e2.Submit(context.Background(), engine.SubmitRequest{Plan: "kept", Tenant: "t"}); err != nil {
		t.Fatalf("the plan, loaded from the store: %v", err)
	}
}

// Without a copy in the store, the data directory's catalog goes into it.
func TestCatalogStoreIsSeededFromTheDataDirectory(t *testing.T) {
	dir := t.TempDir()
	e1 := newRegistryEngine(t)
	h := (&API{E: e1, Dir: dir}).Handler()
	if w := postPlanDefinition(t, h, "old", "w", "go"); w.Code != http.StatusOK && w.Code != http.StatusCreated {
		t.Fatalf("register: %d %s", w.Code, w.Body)
	}
	store := blob.NewMem()
	if err := LoadWith(newRegistryEngine(t), dir, store); err != nil {
		t.Fatal(err)
	}
	e3 := newRegistryEngine(t)
	if err := LoadWith(e3, t.TempDir(), store); err != nil {
		t.Fatal(err)
	}
	if err := e3.Start(); err != nil {
		t.Fatal(err)
	}
	if _, err := e3.Submit(context.Background(), engine.SubmitRequest{Plan: "old", Tenant: "t"}); err != nil {
		t.Fatalf("the plan, seeded into the store: %v", err)
	}
}
