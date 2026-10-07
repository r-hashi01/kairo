// kairo-n8n runs n8n's engine v2 data plane on kairo (ADR 0042): the
// engine, a socket for n8n's node workers, the HTTP API n8n's control plane
// calls (the same as engine v2's), and the records of executions in
// PostgreSQL.
//
// Configuration comes from the environment, with engine v2's names where
// there are ones:
//
//	N8N_ENGINE_AUTH_SECRET             shared with the control plane (>= 32 characters)
//	N8N_ENGINE_DATABASE_URL            PostgreSQL for the records (verified TLS unless -insecure-db)
//	N8N_ENGINE_CONTROL_PLANE_BASE_URL  where lifecycle events go (unset: none are sent)
//	KAIRO_N8N_REDIS_ADDR               Redis for execution responses, host:port (unset: none)
//	KAIRO_N8N_REDIS_PASSWORD, KAIRO_N8N_REDIS_PREFIX (default "n8n")
//	KAIRO_WORKER_TOKEN                 required in workers' Hello (default: the auth secret)
//
// -keys encrypts what the engine stores in the data directory (ADR 0021),
// as for kairo-dify.
package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"
	"time"

	n8n "kairo/compat/n8n"
	"kairo/engine"
	"kairo/ir"
	"kairo/protocol"
	"kairo/seal"
	"kairo/store/postgres"
)

func main() {
	var (
		dataDir    = flag.String("data", "./kairo-n8n-data", "data directory (logs, snapshots, plans)")
		shards     = flag.Int("shards", 16, "number of shards (fixed per data directory)")
		httpAddr   = flag.String("http", "127.0.0.1:3000", "API listen address (engine v2's N8N_ENGINE_PORT)")
		sockPath   = flag.String("socket", "", "UNIX socket for workers (default: <data>/worker.sock)")
		tcpAddr    = flag.String("worker-tcp", "", "optional TCP address for workers")
		noSync     = flag.Bool("nosync", false, "skip fsync (only for tmpfs or testing)")
		prefix     = flag.String("table-prefix", "kairo_n8n_", "prefix of the record tables")
		insecureDB = flag.Bool("insecure-db", false, "allow a database connection without verified TLS (local testing only)")
		keysFile   = flag.String("keys", "", "JSON file with the keys that encrypt stored data")
		feedHold   = flag.Int("feed-hold", 10000, "hold back new runs while more run events than this await recording (ADR 0039)")
	)
	flag.Parse()

	secret := os.Getenv("N8N_ENGINE_AUTH_SECRET")
	if len(secret) < n8n.MinSecret {
		log.Fatalf("kairo-n8n: N8N_ENGINE_AUTH_SECRET must have at least %d characters", n8n.MinSecret)
	}
	dsn := os.Getenv("N8N_ENGINE_DATABASE_URL")
	if dsn == "" {
		log.Fatal("kairo-n8n: N8N_ENGINE_DATABASE_URL is not set")
	}
	db, err := postgres.Open(dsn, postgres.Options{AllowInsecureTransport: *insecureDB})
	if err != nil {
		log.Fatal(err)
	}
	views, err := n8n.OpenViews(db, *prefix)
	if err != nil {
		log.Fatalf("kairo-n8n: records: %v", err)
	}
	workerToken := os.Getenv("KAIRO_WORKER_TOKEN")
	if workerToken == "" {
		workerToken = secret
	}
	var keys seal.Keys
	if *keysFile != "" {
		if keys, err = loadKeys(*keysFile); err != nil {
			log.Fatalf("kairo-n8n: %s: %v", *keysFile, err)
		}
	}
	reg := ir.NewRegistry()
	n8n.Spec(reg)
	e, err := engine.New(engine.Config{
		Keys:        keys,
		Shards:      *shards,
		Registry:    reg,
		DataDir:     *dataDir,
		NoSync:      *noSync,
		DefaultTier: engine.TierFile,
		Feeds:       []string{n8n.FeedName},
		FeedHold:    *feedHold,
		// kairo.slice cannot read a list kept as a blob (ADR 0043): outputs
		// stay in the state, as engine v2 keeps them in its rows.
		BlobThreshold: 1 << 30,
	})
	if err != nil {
		log.Fatal(err)
	}
	events := &n8n.Events{URL: os.Getenv("N8N_ENGINE_CONTROL_PLANE_BASE_URL"), Secret: []byte(secret)}
	var responses *n8n.Responses
	if addr := os.Getenv("KAIRO_N8N_REDIS_ADDR"); addr != "" {
		responses = &n8n.Responses{Addr: addr, Password: os.Getenv("KAIRO_N8N_REDIS_PASSWORD"), Prefix: os.Getenv("KAIRO_N8N_REDIS_PREFIX")}
	}
	d := &n8n.Daemon{E: e, Views: views, Secret: []byte(secret), Events: events, Responses: responses, Dir: *dataDir,
		WorkerToken: workerToken}
	if err := d.Load(); err != nil {
		log.Fatal(err)
	}
	if err := e.Start(); err != nil {
		log.Fatal(err)
	}
	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	go events.Run(ctx)
	go func() {
		if err := d.Consume(ctx); err != nil {
			log.Fatalf("kairo-n8n: event feed: %v", err)
		}
	}()

	ws := &protocol.Server{E: e, Token: workerToken}
	if *sockPath == "" {
		*sockPath = filepath.Join(*dataDir, "worker.sock")
	}
	os.Remove(*sockPath)
	ul, err := net.Listen("unix", *sockPath)
	if err != nil {
		log.Fatal(err)
	}
	os.Chmod(*sockPath, 0o600)
	go ws.Serve(ul)
	if *tcpAddr != "" {
		tl, err := net.Listen("tcp", *tcpAddr)
		if err != nil {
			log.Fatal(err)
		}
		go ws.Serve(tl)
	}
	srv := &http.Server{Addr: *httpAddr, Handler: d.Handler(), ReadHeaderTimeout: 10 * time.Second}
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatal(err)
		}
	}()
	log.Printf("kairo-n8n: api on http://%s, workers on unix:%s, data in %s", *httpAddr, *sockPath, *dataDir)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	log.Printf("kairo-n8n: shutting down")
	sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	srv.Shutdown(sctx)
	ul.Close()
	stop()
	e.Close()
	db.Close()
}

func loadKeys(path string) (seal.Keys, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var f struct {
		Current uint32            `json:"current"`
		Keys    map[string]string `json:"keys"`
		NameKey string            `json:"name_key"`
	}
	if err := json.Unmarshal(b, &f); err != nil {
		return nil, err
	}
	keys := map[uint32][]byte{}
	for id, k := range f.Keys {
		n, err := strconv.ParseUint(id, 10, 32)
		if err != nil {
			return nil, fmt.Errorf("key id %q: %w", id, err)
		}
		if keys[uint32(n)], err = base64.StdEncoding.DecodeString(k); err != nil {
			return nil, fmt.Errorf("key %q: %w", id, err)
		}
	}
	name, err := base64.StdEncoding.DecodeString(f.NameKey)
	if err != nil {
		return nil, fmt.Errorf("name_key: %w", err)
	}
	return seal.NewStatic(f.Current, keys, name)
}
