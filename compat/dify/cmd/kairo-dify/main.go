// kairo-dify runs Dify workflows on kairo (ADR 0037): the engine, a socket
// for Dify's node workers, and the HTTP API Dify's adapter calls.
//
// Secrets come from the environment: KAIRO_DIFY_API_KEY (required by the
// API as a Bearer token) and KAIRO_WORKER_TOKEN (required in workers'
// Hello; defaults to the API key). Listen on internal addresses only; put
// a TLS proxy in front when the network is not trusted.
//
// Runs' inputs (with the workflow's environment variables, secrets
// included) are stored in the data directory. -keys encrypts everything
// stored there (ADR 0021): a JSON file
//
//	{"current": 1, "keys": {"1": "<base64, 32 bytes>"}, "name_key": "<base64, >= 32 bytes>"}
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

	"github.com/r-hashi01/kairo/compat/dify/daemon"
	"github.com/r-hashi01/kairo/engine"
	"github.com/r-hashi01/kairo/ir"
	"github.com/r-hashi01/kairo/protocol"
	"github.com/r-hashi01/kairo/sched"
	"github.com/r-hashi01/kairo/seal"
)

func main() {
	var (
		dataDir   = flag.String("data", "./kairo-dify-data", "data directory (logs, snapshots, blobs, plans)")
		shards    = flag.Int("shards", 64, "number of shards, also the number of event partitions workers share (fixed per data directory)")
		httpAddr  = flag.String("http", "127.0.0.1:8430", "API listen address")
		sockPath  = flag.String("socket", "", "UNIX socket for workers (default: <data>/worker.sock)")
		tcpAddr   = flag.String("worker-tcp", "", "optional TCP address for workers")
		tierName  = flag.String("tier", "file", "durability tier of runs: none|memory|file")
		noSync    = flag.Bool("nosync", false, "skip fsync (only for tmpfs or testing)")
		maxActive = flag.Int("max-active", 0, "admission: max concurrently active runs (0 = unlimited)")
		maxQueued = flag.Int("max-queued", 100000, "admission: max runs waiting for admission")
		perTenant = flag.Int("tenant-max-active", 0, "admission: max active runs per tenant (0 = unlimited)")
		idemTTL   = flag.Duration("idempotency-ttl", 24*time.Hour, "how long finished run ids stay idempotency keys")
		noAuth    = flag.Bool("insecure-no-auth", false, "serve without KAIRO_DIFY_API_KEY (local testing only)")
		keysFile  = flag.String("keys", "", "JSON file with the keys that encrypt stored data")
		feedHold  = flag.Int("feed-hold", 10000, "hold back new runs (runs in progress go on) while more run events than this await Dify's workers (ADR 0039)")
	)
	flag.Parse()

	key := os.Getenv("KAIRO_DIFY_API_KEY")
	if key == "" && !*noAuth {
		log.Fatal("kairo-dify: KAIRO_DIFY_API_KEY is not set (or pass -insecure-no-auth for local testing)")
	}
	workerToken := os.Getenv("KAIRO_WORKER_TOKEN")
	if workerToken == "" {
		workerToken = key
	}
	var tier engine.Tier
	if err := tier.UnmarshalText([]byte(*tierName)); err != nil {
		log.Fatal(err)
	}
	var keys seal.Keys
	if *keysFile != "" {
		k, err := loadKeys(*keysFile)
		if err != nil {
			log.Fatalf("kairo-dify: %s: %v", *keysFile, err)
		}
		keys = k
	}
	e, err := engine.New(engine.Config{
		Keys:           keys,
		Shards:         *shards,
		Registry:       ir.NewRegistry(),
		DataDir:        *dataDir,
		NoSync:         *noSync,
		DefaultTier:    tier,
		IdempotencyTTL: *idemTTL,
		Feeds:          []string{daemon.FeedName},
		FeedHold:       *feedHold,
		Admission: sched.AdmissionConfig{
			MaxActive: *maxActive, MaxQueued: *maxQueued,
			MaxActivePerTenant: func(string) int { return *perTenant },
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	d := &daemon.Daemon{E: e, Key: key, Dir: *dataDir}
	if err := d.Load(); err != nil {
		log.Fatal(err)
	}
	if err := e.Start(); err != nil {
		log.Fatal(err)
	}
	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	go func() {
		if err := d.Consume(ctx); err != nil {
			log.Fatalf("kairo-dify: event feed: %v", err)
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
	log.Printf("kairo-dify: api on http://%s, workers on unix:%s, data in %s", *httpAddr, *sockPath, *dataDir)

	logStatsOnSignal(e, d)
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	log.Printf("kairo-dify: shutting down")
	sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	srv.Shutdown(sctx)
	ul.Close()
	stop()
	e.Close()
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
