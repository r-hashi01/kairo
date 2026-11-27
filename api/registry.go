package api

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sync"

	"github.com/r-hashi01/kairo/blob"
	"github.com/r-hashi01/kairo/engine"
	"github.com/r-hashi01/kairo/ir"
)

const (
	registryCatalogVersion = 1
	registryCatalogFile    = "registry.json"
)

type registryState struct {
	mu        sync.Mutex
	uncertain bool
}

type registryCatalog struct {
	Version int              `json:"version"`
	Nodes   []ir.NodeSpec    `json:"nodes,omitempty"`
	Plans   []catalogPlan    `json:"plans,omitempty"`
	Current []catalogCurrent `json:"current,omitempty"`
}

type catalogPlan struct {
	Name   string    `json:"name"`
	Hash   string    `json:"hash"`
	Frozen ir.Frozen `json:"frozen"`
}

type catalogCurrent struct {
	Name string `json:"name"`
	Hash string `json:"hash"`
}

type planKey struct {
	name string
	hash string
}

type catalogFileOps struct {
	syncFile func(*os.File) error
	rename   func(string, string) error
	syncDir  func(string) error
}

type catalogCommitUnknownError struct{ err error }

func (e *catalogCommitUnknownError) Error() string {
	return "registry catalog replacement has unknown durability: " + e.err.Error()
}

func (e *catalogCommitUnknownError) Unwrap() error { return e.err }

func defaultCatalogFileOps() catalogFileOps {
	return catalogFileOps{
		syncFile: func(f *os.File) error { return f.Sync() },
		rename:   os.Rename,
		syncDir:  syncDirectory,
	}
}

func (a *API) fileOps() catalogFileOps {
	if a.catalogOps != nil {
		return *a.catalogOps
	}
	return defaultCatalogFileOps()
}

func (a *API) markRegistryWriteError(err error) {
	var unknown *catalogCommitUnknownError
	if errors.As(err, &unknown) {
		a.registry.uncertain = true
	}
}

func (a *API) registryUnavailable() bool {
	a.registry.mu.Lock()
	defer a.registry.mu.Unlock()
	return a.registry.uncertain
}

func registryUnavailableError() error {
	return errors.New("registry durability is unknown; restart before registering plans or starting runs")
}

func syncDirectory(path string) error {
	d, err := os.Open(path)
	if err != nil {
		return err
	}
	if err := d.Sync(); err != nil {
		d.Close()
		return err
	}
	return d.Close()
}

// registryCatalogKey names the catalog in API.Catalog.
const registryCatalogKey = "kairod/registry.json"

// writeCatalog keeps the catalog in Catalog first (ADR 0087), then in Dir:
// written to the store and not to the file, it is the store's copy that a
// restart loads (LoadWith).
func (a *API) writeCatalog(c *registryCatalog) error {
	if a.Catalog != nil {
		normalizeCatalog(c)
		b, err := json.MarshalIndent(c, "", "  ")
		if err != nil {
			return err
		}
		if err := a.Catalog.Put(registryCatalogKey, append(b, '\n')); err != nil {
			return err
		}
	}
	return writeRegistryCatalog(a.Dir, c, a.fileOps())
}

// LoadWith is Load, with the catalog kept in store too (API.Catalog, ADR
// 0087): the store's copy, if there is one, replaces dir's (a kairod on
// another machine, its data directory new or lost); else dir's goes into
// the store.
func LoadWith(e *engine.Engine, dir string, store blob.Store) error {
	if dir != "" && store != nil {
		if err := syncCatalog(dir, store); err != nil {
			return err
		}
	}
	return Load(e, dir)
}

func syncCatalog(dir string, store blob.Store) error {
	b, err := store.Get(registryCatalogKey)
	if errors.Is(err, blob.ErrNotFound) {
		c, err := loadOrMigrateRegistry(dir, defaultCatalogFileOps())
		if err != nil {
			return err
		}
		normalizeCatalog(&c)
		b, err := json.MarshalIndent(&c, "", "  ")
		if err != nil {
			return err
		}
		return store.Put(registryCatalogKey, append(b, '\n'))
	}
	if err != nil {
		return err
	}
	var c registryCatalog
	if err := json.Unmarshal(b, &c); err != nil {
		return fmt.Errorf("the store's registry catalog: %w", err)
	}
	if c.Version != registryCatalogVersion {
		return fmt.Errorf("the store's registry catalog: unsupported version %d", c.Version)
	}
	if _, err := validateRegistryCatalog(&c); err != nil {
		return err
	}
	return writeRegistryCatalog(dir, &c, defaultCatalogFileOps())
}

func writeRegistryCatalog(dir string, c *registryCatalog, ops catalogFileOps) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	normalizeCatalog(c)
	b, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return err
	}
	b = append(b, '\n')
	path := filepath.Join(dir, registryCatalogFile)
	f, err := os.CreateTemp(dir, ".registry.json.tmp-")
	if err != nil {
		return err
	}
	tmp := f.Name()
	closed := false
	defer func() {
		if !closed {
			f.Close()
		}
		os.Remove(tmp)
	}()
	if err := f.Chmod(0o644); err != nil {
		return err
	}
	if _, err := f.Write(b); err != nil {
		return err
	}
	if err := ops.syncFile(f); err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		closed = true
		return err
	}
	closed = true
	if err := ops.rename(tmp, path); err != nil {
		return err
	}
	if err := ops.syncDir(dir); err != nil {
		return &catalogCommitUnknownError{err: err}
	}
	return nil
}

func normalizeCatalog(c *registryCatalog) {
	c.Version = registryCatalogVersion
	slices.SortFunc(c.Nodes, func(a, b ir.NodeSpec) int { return bytes.Compare([]byte(a.Action), []byte(b.Action)) })
	slices.SortFunc(c.Plans, func(a, b catalogPlan) int {
		if n := bytes.Compare([]byte(a.Name), []byte(b.Name)); n != 0 {
			return n
		}
		return bytes.Compare([]byte(a.Hash), []byte(b.Hash))
	})
	slices.SortFunc(c.Current, func(a, b catalogCurrent) int { return bytes.Compare([]byte(a.Name), []byte(b.Name)) })
}

func readRegistryCatalog(dir string) (registryCatalog, bool, error) {
	path := filepath.Join(dir, registryCatalogFile)
	b, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return registryCatalog{Version: registryCatalogVersion}, false, nil
	}
	if err != nil {
		return registryCatalog{}, false, err
	}
	var c registryCatalog
	if err := json.Unmarshal(b, &c); err != nil {
		return registryCatalog{}, true, fmt.Errorf("%s: %w", path, err)
	}
	if c.Version != registryCatalogVersion {
		return registryCatalog{}, true, fmt.Errorf("%s: unsupported registry catalog version %d", path, c.Version)
	}
	return c, true, nil
}

func validateRegistryCatalog(c *registryCatalog) (map[planKey]*ir.Plan, error) {
	nodes := map[string][]byte{}
	for _, s := range c.Nodes {
		if s.Action == "" {
			return nil, errors.New("registry catalog: node spec without action")
		}
		b, err := json.Marshal(s)
		if err != nil {
			return nil, err
		}
		if prev, ok := nodes[s.Action]; ok {
			if !bytes.Equal(prev, b) {
				return nil, fmt.Errorf("registry catalog: conflicting node specs for %q", s.Action)
			}
			return nil, fmt.Errorf("registry catalog: duplicate node spec %q", s.Action)
		}
		nodes[s.Action] = b
	}

	current := map[string]string{}
	currentKeys := map[planKey]bool{}
	for _, cur := range c.Current {
		if cur.Name == "" || cur.Hash == "" {
			return nil, errors.New("registry catalog: current plan needs name and hash")
		}
		if _, ok := current[cur.Name]; ok {
			return nil, fmt.Errorf("registry catalog: duplicate current plan %q", cur.Name)
		}
		current[cur.Name] = cur.Hash
		currentKeys[planKey{cur.Name, cur.Hash}] = true
	}

	seen := map[planKey][]byte{}
	loaded := map[planKey]*ir.Plan{}
	for _, entry := range c.Plans {
		p, err := entry.Frozen.Thaw()
		if err != nil {
			return nil, fmt.Errorf("registry catalog: plan %s@%s: %w", entry.Name, entry.Hash, err)
		}
		if p.Name != entry.Name || p.Hash != entry.Hash {
			return nil, fmt.Errorf("registry catalog: plan identity %s@%s contains %s@%s", entry.Name, entry.Hash, p.Name, p.Hash)
		}
		key := planKey{entry.Name, entry.Hash}
		b, err := json.Marshal(entry.Frozen)
		if err != nil {
			return nil, err
		}
		if prev, ok := seen[key]; ok {
			if !bytes.Equal(prev, b) {
				return nil, fmt.Errorf("registry catalog: conflicting frozen plans for %s@%s", entry.Name, entry.Hash)
			}
			return nil, fmt.Errorf("registry catalog: duplicate frozen plan %s@%s", entry.Name, entry.Hash)
		}
		seen[key] = b
		if currentKeys[key] {
			loaded[key] = p
		}
	}
	for key := range currentKeys {
		if _, ok := seen[key]; !ok {
			return nil, fmt.Errorf("registry catalog: current plan %s@%s is missing", key.name, key.hash)
		}
	}
	return loaded, nil
}

func loadOrMigrateRegistry(dir string, ops catalogFileOps) (registryCatalog, error) {
	c, found, err := readRegistryCatalog(dir)
	if err != nil {
		return registryCatalog{}, err
	}
	if found {
		if _, err := validateRegistryCatalog(&c); err != nil {
			return registryCatalog{}, err
		}
		return c, nil
	}
	c, found, err = readLegacyRegistry(dir)
	if err != nil {
		return registryCatalog{}, err
	}
	if !found {
		return registryCatalog{Version: registryCatalogVersion}, nil
	}
	if _, err := validateRegistryCatalog(&c); err != nil {
		return registryCatalog{}, err
	}
	if err := writeRegistryCatalog(dir, &c, ops); err != nil {
		return registryCatalog{}, fmt.Errorf("migrate registry catalog: %w", err)
	}
	return c, nil
}

func readLegacyRegistry(dir string) (registryCatalog, bool, error) {
	c := registryCatalog{Version: registryCatalogVersion}
	nodeFiles, err := filepath.Glob(filepath.Join(dir, "nodes", "*.json"))
	if err != nil {
		return c, false, err
	}
	planFiles, err := filepath.Glob(filepath.Join(dir, "plans", "*.json"))
	if err != nil {
		return c, false, err
	}
	if len(nodeFiles) == 0 && len(planFiles) == 0 {
		return c, false, nil
	}
	nodes := map[string][]byte{}
	for _, path := range nodeFiles {
		b, err := os.ReadFile(path)
		if err != nil {
			return c, true, err
		}
		var s ir.NodeSpec
		if err := json.Unmarshal(b, &s); err != nil {
			return c, true, fmt.Errorf("%s: %w", path, err)
		}
		if s.Action == "" {
			return c, true, fmt.Errorf("%s: node spec without action", path)
		}
		canonical, err := json.Marshal(s)
		if err != nil {
			return c, true, err
		}
		if prev, ok := nodes[s.Action]; ok {
			if !bytes.Equal(prev, canonical) {
				return c, true, fmt.Errorf("legacy registry: conflicting node specs for %q", s.Action)
			}
			continue
		}
		nodes[s.Action] = canonical
		c.Nodes = append(c.Nodes, s)
	}

	plans := map[planKey][]byte{}
	versions := map[string][]string{}
	for _, path := range planFiles {
		b, err := os.ReadFile(path)
		if err != nil {
			return c, true, err
		}
		var frozen ir.Frozen
		if err := json.Unmarshal(b, &frozen); err != nil {
			return c, true, fmt.Errorf("%s: %w", path, err)
		}
		p, err := frozen.Thaw()
		if err != nil {
			return c, true, fmt.Errorf("%s: %w", path, err)
		}
		key := planKey{p.Name, p.Hash}
		canonical, err := json.Marshal(frozen)
		if err != nil {
			return c, true, err
		}
		if prev, ok := plans[key]; ok {
			if !bytes.Equal(prev, canonical) {
				return c, true, fmt.Errorf("legacy registry: conflicting frozen plans for %s@%s", p.Name, p.Hash)
			}
			continue
		}
		plans[key] = canonical
		versions[p.Name] = append(versions[p.Name], p.Hash)
		c.Plans = append(c.Plans, catalogPlan{Name: p.Name, Hash: p.Hash, Frozen: frozen})
	}
	for name, hashes := range versions {
		if len(hashes) == 1 {
			c.Current = append(c.Current, catalogCurrent{Name: name, Hash: hashes[0]})
		}
	}
	normalizeCatalog(&c)
	return c, true, nil
}

func upsertCatalogNodes(c *registryCatalog, specs []ir.NodeSpec) {
	index := make(map[string]int, len(c.Nodes))
	for i := range c.Nodes {
		index[c.Nodes[i].Action] = i
	}
	for _, s := range specs {
		if i, ok := index[s.Action]; ok {
			c.Nodes[i] = s
			continue
		}
		index[s.Action] = len(c.Nodes)
		c.Nodes = append(c.Nodes, s)
	}
}

func upsertCatalogPlan(c *registryCatalog, p *ir.Plan) error {
	frozen := p.Freeze()
	want, err := json.Marshal(frozen)
	if err != nil {
		return err
	}
	found := false
	for _, entry := range c.Plans {
		if entry.Name != p.Name || entry.Hash != p.Hash {
			continue
		}
		got, err := json.Marshal(entry.Frozen)
		if err != nil {
			return err
		}
		if !bytes.Equal(got, want) {
			return fmt.Errorf("registry catalog: plan hash collision for %s@%s", p.Name, p.Hash)
		}
		found = true
		break
	}
	if !found {
		c.Plans = append(c.Plans, catalogPlan{Name: p.Name, Hash: p.Hash, Frozen: frozen})
	}
	for i := range c.Current {
		if c.Current[i].Name == p.Name {
			c.Current[i].Hash = p.Hash
			return nil
		}
	}
	c.Current = append(c.Current, catalogCurrent{Name: p.Name, Hash: p.Hash})
	return nil
}

func loadCatalogPlan(dir, name, hash string) (*ir.Plan, error) {
	c, found, err := readRegistryCatalog(dir)
	if err != nil || !found {
		return nil, err
	}
	var matched *ir.Frozen
	for i := range c.Plans {
		entry := &c.Plans[i]
		if entry.Name != name || entry.Hash != hash {
			continue
		}
		if matched != nil {
			return nil, fmt.Errorf("registry catalog: duplicate frozen plan %s@%s", name, hash)
		}
		matched = &entry.Frozen
	}
	if matched == nil {
		return nil, nil
	}
	p, err := matched.Thaw()
	if err != nil {
		return nil, err
	}
	if p.Name != name || p.Hash != hash {
		return nil, fmt.Errorf("registry catalog: plan identity %s@%s contains %s@%s", name, hash, p.Name, p.Hash)
	}
	return p, nil
}

// Load restores the durable registry. The catalog remains the source of
// truth; only current plans enter memory here. Historical plans are loaded by
// name and hash when Submit or startup recovery actually references them.
func Load(e *engine.Engine, dir string) error {
	if dir == "" {
		return nil
	}
	c, err := loadOrMigrateRegistry(dir, defaultCatalogFileOps())
	if err != nil {
		return err
	}
	current, err := validateRegistryCatalog(&c)
	if err != nil {
		return err
	}
	for _, s := range c.Nodes {
		e.Registry().Register(s)
	}
	for _, cur := range c.Current {
		e.AddPlan(current[planKey{cur.Name, cur.Hash}], true)
	}
	e.SetPlanLoader(func(name, hash string) (*ir.Plan, error) {
		return loadCatalogPlan(dir, name, hash)
	})
	return nil
}
