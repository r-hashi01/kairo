package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/r-hashi01/kairo/engine"
	"github.com/r-hashi01/kairo/ir"
)

func newRegistryEngine(t *testing.T) *engine.Engine {
	t.Helper()
	e, err := engine.New(engine.Config{Shards: 1, NoSync: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(e.Close)
	return e
}

func postRegistryJSON(t *testing.T, h http.Handler, path string, body []byte) *httptest.ResponseRecorder {
	t.Helper()
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodPost, path, bytes.NewReader(body)))
	return w
}

func postPlanDefinition(t *testing.T, h http.Handler, name, waitID, signal string) *httptest.ResponseRecorder {
	t.Helper()
	body := fmt.Appendf(nil, `{"name":%q,"root":{"kind":"wait","id":%q,"signal":%q}}`, name, waitID, signal)
	return postRegistryJSON(t, h, "/v1/plans", body)
}

func compileWaitPlan(t *testing.T, name, waitID, signal string) *ir.Plan {
	t.Helper()
	var def ir.Definition
	b := fmt.Appendf(nil, `{"name":%q,"root":{"kind":"wait","id":%q,"signal":%q}}`, name, waitID, signal)
	if err := json.Unmarshal(b, &def); err != nil {
		t.Fatal(err)
	}
	p, err := ir.Compile(&def, ir.NewRegistry())
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func TestWriteRegistryCatalogDurabilityOrder(t *testing.T) {
	dir := t.TempDir()
	c := registryCatalog{Nodes: []ir.NodeSpec{{Action: "mail/send", Effect: ir.EffectUnprotected}}}
	base := defaultCatalogFileOps()
	var order []string
	ops := catalogFileOps{
		syncFile: func(f *os.File) error {
			order = append(order, "file-sync")
			return base.syncFile(f)
		},
		rename: func(old, new string) error {
			order = append(order, "rename")
			return base.rename(old, new)
		},
		syncDir: func(path string) error {
			order = append(order, "dir-sync")
			return base.syncDir(path)
		},
	}
	if err := writeRegistryCatalog(dir, &c, ops); err != nil {
		t.Fatal(err)
	}
	if want := []string{"file-sync", "rename", "dir-sync"}; !slices.Equal(order, want) {
		t.Fatalf("durability order = %v, want %v", order, want)
	}
	got, found, err := readRegistryCatalog(dir)
	if err != nil || !found || len(got.Nodes) != 1 || got.Nodes[0].Action != "mail/send" {
		t.Fatalf("catalog = %+v, found=%v, err=%v", got, found, err)
	}
}

func TestWriteRegistryCatalogFileSyncFailureKeepsPreviousCatalog(t *testing.T) {
	dir := t.TempDir()
	old := registryCatalog{Nodes: []ir.NodeSpec{{Action: "old"}}}
	if err := writeRegistryCatalog(dir, &old, defaultCatalogFileOps()); err != nil {
		t.Fatal(err)
	}
	base := defaultCatalogFileOps()
	ops := base
	ops.syncFile = func(*os.File) error { return errors.New("injected file sync failure") }
	next := registryCatalog{Nodes: []ir.NodeSpec{{Action: "new"}}}
	if err := writeRegistryCatalog(dir, &next, ops); err == nil {
		t.Fatal("write succeeded")
	}
	got, found, err := readRegistryCatalog(dir)
	if err != nil || !found || len(got.Nodes) != 1 || got.Nodes[0].Action != "old" {
		t.Fatalf("catalog after failed write = %+v, found=%v, err=%v", got, found, err)
	}
}

func TestDirectorySyncFailureBlocksRegistrationAndSubmit(t *testing.T) {
	dir := t.TempDir()
	e := newRegistryEngine(t)
	ops := defaultCatalogFileOps()
	ops.syncDir = func(string) error { return errors.New("injected directory sync failure") }
	a := &API{E: e, Dir: dir, catalogOps: &ops}
	h := a.Handler()
	if w := postPlanDefinition(t, h, "orders", "wait", "done"); w.Code != http.StatusInternalServerError {
		t.Fatalf("register status=%d body=%s", w.Code, w.Body.String())
	}
	if _, found, err := readRegistryCatalog(dir); err != nil || !found {
		t.Fatalf("replaced catalog found=%v err=%v", found, err)
	}
	w := postRegistryJSON(t, h, "/v1/runs", []byte(`{"plan":"orders","run_id":"r"}`))
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("submit status=%d body=%s", w.Code, w.Body.String())
	}
	if w := postPlanDefinition(t, h, "other", "wait", "done"); w.Code != http.StatusServiceUnavailable {
		t.Fatalf("second register status=%d body=%s", w.Code, w.Body.String())
	}
}

func TestRegistrationWriteFailureDoesNotChangeMemory(t *testing.T) {
	dir := t.TempDir()
	e := newRegistryEngine(t)
	ops := defaultCatalogFileOps()
	ops.syncFile = func(*os.File) error { return errors.New("injected file sync failure") }
	notified := 0
	a := &API{E: e, Dir: dir, catalogOps: &ops, OnNode: func(ir.NodeSpec) { notified++ }}
	h := a.Handler()
	w := postRegistryJSON(t, h, "/v1/nodes", []byte(`{"action":"mail/send","effect":"unprotected"}`))
	if w.Code != http.StatusInternalServerError {
		t.Fatalf("node status=%d body=%s", w.Code, w.Body.String())
	}
	if got := e.Registry().Lookup("mail/send").Effect; got != ir.EffectReal {
		t.Fatalf("failed node registration became visible: %s", got)
	}
	if notified != 0 {
		t.Fatalf("OnNode called %d times", notified)
	}
	if w := postPlanDefinition(t, h, "orders", "wait", "done"); w.Code != http.StatusInternalServerError {
		t.Fatalf("plan status=%d body=%s", w.Code, w.Body.String())
	}
	if _, err := e.Submit(context.Background(), engine.SubmitRequest{Plan: "orders", RunID: "r"}); !errors.Is(err, engine.ErrUnknownPlan) {
		t.Fatalf("failed plan registration Submit error = %v", err)
	}
}

func TestConcurrentRegistryUpdatesDoNotLoseEntries(t *testing.T) {
	dir := t.TempDir()
	e := newRegistryEngine(t)
	ops := defaultCatalogFileOps()
	ops.syncFile = func(*os.File) error { return nil }
	ops.syncDir = func(string) error { return nil }
	h := (&API{E: e, Dir: dir, catalogOps: &ops}).Handler()
	const registrations = 16
	errs := make(chan string, registrations*2)
	var wg sync.WaitGroup
	for i := range registrations {
		i := i
		wg.Add(2)
		go func() {
			defer wg.Done()
			body := fmt.Appendf(nil, `{"action":"action/%d","effect":"unprotected"}`, i)
			if w := postRegistryJSON(t, h, "/v1/nodes", body); w.Code != http.StatusOK {
				errs <- fmt.Sprintf("node %d: status=%d body=%s", i, w.Code, w.Body.String())
			}
		}()
		go func() {
			defer wg.Done()
			if w := postPlanDefinition(t, h, fmt.Sprintf("plan/%d", i), "wait", "done"); w.Code != http.StatusOK {
				errs <- fmt.Sprintf("plan %d: status=%d body=%s", i, w.Code, w.Body.String())
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	catalog, found, err := readRegistryCatalog(dir)
	if err != nil || !found {
		t.Fatalf("catalog found=%v err=%v", found, err)
	}
	if len(catalog.Nodes) != registrations || len(catalog.Plans) != registrations || len(catalog.Current) != registrations {
		t.Fatalf("nodes=%d plans=%d current=%d", len(catalog.Nodes), len(catalog.Plans), len(catalog.Current))
	}
}

func TestExactNamesDoNotCollideAndSurviveRestart(t *testing.T) {
	dir := t.TempDir()
	e1 := newRegistryEngine(t)
	a := &API{E: e1, Dir: dir}
	h := a.Handler()
	names := []string{"name/part", "name:part", "name@part", "name_part"}
	specs := make([]ir.NodeSpec, 0, len(names))
	for _, name := range names {
		specs = append(specs, ir.NodeSpec{Action: name, Effect: ir.EffectUnprotected})
	}
	b, err := json.Marshal(specs)
	if err != nil {
		t.Fatal(err)
	}
	if w := postRegistryJSON(t, h, "/v1/nodes", b); w.Code != http.StatusOK {
		t.Fatalf("nodes status=%d body=%s", w.Code, w.Body.String())
	}
	for i, name := range names {
		if w := postPlanDefinition(t, h, name, fmt.Sprintf("wait-%d", i), "done"); w.Code != http.StatusOK {
			t.Fatalf("plan %q status=%d body=%s", name, w.Code, w.Body.String())
		}
	}
	catalog, found, err := readRegistryCatalog(dir)
	if err != nil || !found || len(catalog.Nodes) != len(names) || len(catalog.Current) != len(names) {
		t.Fatalf("catalog nodes=%d current=%d found=%v err=%v", len(catalog.Nodes), len(catalog.Current), found, err)
	}

	e2 := newRegistryEngine(t)
	if err := Load(e2, dir); err != nil {
		t.Fatal(err)
	}
	if err := e2.Start(); err != nil {
		t.Fatal(err)
	}
	for i, name := range names {
		if got := e2.Registry().Lookup(name).Effect; got != ir.EffectUnprotected {
			t.Fatalf("node %q effect = %s", name, got)
		}
		if _, err := e2.Submit(context.Background(), engine.SubmitRequest{Plan: name, RunID: fmt.Sprintf("run-%d", i)}); err != nil {
			t.Fatalf("submit %q: %v", name, err)
		}
	}
}

func TestExplicitCurrentPlanSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	e1 := newRegistryEngine(t)
	h := (&API{E: e1, Dir: dir}).Handler()
	if w := postPlanDefinition(t, h, "orders", "old-wait", "old"); w.Code != http.StatusOK {
		t.Fatalf("old status=%d body=%s", w.Code, w.Body.String())
	}
	if w := postPlanDefinition(t, h, "orders", "new-wait", "new"); w.Code != http.StatusOK {
		t.Fatalf("new status=%d body=%s", w.Code, w.Body.String())
	}
	e2 := newRegistryEngine(t)
	if err := Load(e2, dir); err != nil {
		t.Fatal(err)
	}
	if err := e2.Start(); err != nil {
		t.Fatal(err)
	}
	if _, err := e2.Submit(context.Background(), engine.SubmitRequest{Plan: "orders", RunID: "current"}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	ri, err := e2.WaitQuiescent(ctx, "current")
	if err != nil {
		t.Fatal(err)
	}
	if len(ri.Waits) != 1 || ri.Waits[0].Signal != "new" {
		t.Fatalf("current waits = %+v", ri.Waits)
	}
}

func TestRecoveryLoadsTheFrozenPlanNamedByTheWAL(t *testing.T) {
	dir := t.TempDir()
	newEngine := func() *engine.Engine {
		e, err := engine.New(engine.Config{
			Shards:      1,
			DataDir:     dir,
			NoSync:      true,
			DefaultTier: engine.TierFile,
		})
		if err != nil {
			t.Fatal(err)
		}
		return e
	}
	e1 := newEngine()
	h := (&API{E: e1, Dir: dir}).Handler()
	if w := postPlanDefinition(t, h, "orders", "old-wait", "old"); w.Code != http.StatusOK {
		t.Fatalf("old status=%d body=%s", w.Code, w.Body.String())
	}
	if err := e1.Start(); err != nil {
		t.Fatal(err)
	}
	if _, err := e1.Submit(context.Background(), engine.SubmitRequest{Plan: "orders", RunID: "started-old"}); err != nil {
		t.Fatal(err)
	}
	if w := postPlanDefinition(t, h, "orders", "new-wait", "new"); w.Code != http.StatusOK {
		t.Fatalf("new status=%d body=%s", w.Code, w.Body.String())
	}
	e1.Close()

	e2 := newEngine()
	defer e2.Close()
	if err := Load(e2, dir); err != nil {
		t.Fatal(err)
	}
	if err := e2.Start(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	oldRun, err := e2.WaitQuiescent(ctx, "started-old")
	if err != nil {
		t.Fatal(err)
	}
	if len(oldRun.Waits) != 1 || oldRun.Waits[0].Signal != "old" {
		t.Fatalf("recovered old waits = %+v", oldRun.Waits)
	}
	if _, err := e2.Submit(context.Background(), engine.SubmitRequest{Plan: "orders", RunID: "started-new"}); err != nil {
		t.Fatal(err)
	}
	newRun, err := e2.WaitQuiescent(ctx, "started-new")
	if err != nil {
		t.Fatal(err)
	}
	if len(newRun.Waits) != 1 || newRun.Waits[0].Signal != "new" {
		t.Fatalf("new current waits = %+v", newRun.Waits)
	}
}

func TestLegacyMigrationDoesNotGuessAmbiguousCurrent(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "nodes"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(dir, "plans"), 0o755); err != nil {
		t.Fatal(err)
	}
	node, err := json.Marshal(ir.NodeSpec{Action: "legacy/action", Effect: ir.EffectUnprotected})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "nodes", "legacy_action.json"), node, 0o644); err != nil {
		t.Fatal(err)
	}
	old := compileWaitPlan(t, "legacy", "old", "old")
	newer := compileWaitPlan(t, "legacy", "new", "new")
	single := compileWaitPlan(t, "single", "only", "only")
	for i, p := range []*ir.Plan{old, newer, single} {
		b, err := json.Marshal(p.Freeze())
		if err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(dir, "plans", fmt.Sprintf("legacy-%d.json", i))
		if err := os.WriteFile(path, b, 0o644); err != nil {
			t.Fatal(err)
		}
		stamp := time.Unix(1_700_000_000, 0)
		if err := os.Chtimes(path, stamp, stamp); err != nil {
			t.Fatal(err)
		}
	}
	e := newRegistryEngine(t)
	if err := Load(e, dir); err != nil {
		t.Fatal(err)
	}
	catalog, found, err := readRegistryCatalog(dir)
	if err != nil || !found {
		t.Fatalf("migrated catalog found=%v err=%v", found, err)
	}
	if len(catalog.Plans) != 3 || len(catalog.Current) != 1 || catalog.Current[0].Name != "single" {
		t.Fatalf("migrated plans=%d current=%+v", len(catalog.Plans), catalog.Current)
	}
	if got := e.Registry().Lookup("legacy/action").Effect; got != ir.EffectUnprotected {
		t.Fatalf("legacy node effect = %s", got)
	}
	if err := e.Start(); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Submit(context.Background(), engine.SubmitRequest{Plan: "legacy", RunID: "ambiguous"}); !errors.Is(err, engine.ErrUnknownPlan) {
		t.Fatalf("ambiguous current Submit error = %v", err)
	}
	if _, err := e.Submit(context.Background(), engine.SubmitRequest{Plan: "single", RunID: "single"}); err != nil {
		t.Fatalf("single current: %v", err)
	}
	for i, p := range []*ir.Plan{old, newer} {
		ref := p.Name + "@" + p.Hash
		if _, err := e.Submit(context.Background(), engine.SubmitRequest{Plan: ref, RunID: fmt.Sprintf("version-%d", i)}); err != nil {
			t.Fatalf("historical %s: %v", ref, err)
		}
	}
}

func TestCatalogRejectsSamePlanIdentityWithDifferentContent(t *testing.T) {
	p := compileWaitPlan(t, "orders", "wait", "done")
	other := compileWaitPlan(t, "other", "wait", "done")
	c := registryCatalog{Plans: []catalogPlan{{Name: p.Name, Hash: p.Hash, Frozen: other.Freeze()}}}
	if err := upsertCatalogPlan(&c, p); err == nil {
		t.Fatal("hash collision accepted")
	}
}
