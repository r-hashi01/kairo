package kairod

import (
	"github.com/r-hashi01/kairo/blob"
	"github.com/r-hashi01/kairo/engine"
)

// Storage opens the SQL database named by dsn (KAIRO_STORE) for the
// durable tiers of a kairod (ADR 0087). Its errors never contain the DSN.
type Storage func(dsn string, o StoreOptions) (*Store, error)

// StoreOptions are the kairod flags that concern its SQL store.
type StoreOptions struct {
	// Namespace separates this kairod's data from others' in the database.
	Namespace string
	// AllowInsecureTransport permits a connection without verified TLS:
	// tests and trusted local setups only (ADR 0020).
	AllowInsecureTransport bool
}

// Store is an opened SQL store.
type Store struct {
	// Configure sets the engine's logs, snapshots and blobs to the store.
	Configure func(*engine.Config)
	// Catalog keeps the plans and node specs registered (api.API.Catalog),
	// so that a kairod on another machine recovers the runs with them.
	Catalog blob.Store
	// Shards pins the number of shards in the store, beside the data it
	// routes (runs are routed by hash: a kairod started on another machine
	// must use the same number). The first kairod stores n; later ones get
	// the stored number, or an error if explicit asks for another n.
	Shards func(n int, explicit bool) (int, error)
	// Close closes the database.
	Close func() error
}
