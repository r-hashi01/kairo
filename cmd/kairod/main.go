// kairod runs the engine as a daemon: an HTTP control API, a socket for
// pulling workers, and the built-in HTTP executor for LLM calls.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"kairo/api"
	"kairo/engine"
	"kairo/executor/httpexec"
	"kairo/ir"
	"kairo/obs"
	"kairo/protocol"
	"kairo/sched"
)

func main() {
	var (
		dataDir   = flag.String("data", "./kairo-data", "data directory (logs, snapshots, blobs, plans)")
		shards    = flag.Int("shards", 0, "number of shards (default: GOMAXPROCS; fixed per data directory)")
		httpAddr  = flag.String("http", "127.0.0.1:8420", "control API listen address")
		sockPath  = flag.String("socket", "", "UNIX socket for workers (default: <data>/worker.sock)")
		tcpAddr   = flag.String("worker-tcp", "", "optional TCP address for workers")
		nodesFile = flag.String("nodes", "", "JSON file with an array of node specs")
		limitFile = flag.String("limits", "", `JSON file: {"<destination>": {"RPM":..,"TPM":..,"TenantConcurrency":..}}`)
		obsFile   = flag.String("obs", "", "write the observability stream as JSON lines to this file (- for stdout)")
		tierName  = flag.String("tier", "none", "default durability tier: none|memory|file")
		noSync    = flag.Bool("nosync", false, "skip fsync (only for tmpfs or testing)")
		maxActive = flag.Int("max-active", 0, "admission: max concurrently active runs (0 = unlimited)")
		maxQueued = flag.Int("max-queued", 100000, "admission: max runs waiting for admission")
		perTenant = flag.Int("tenant-max-active", 0, "admission: max active runs per tenant (0 = unlimited)")
		httpConc  = flag.Int("http-concurrency", 0, "concurrent requests of the built-in HTTP executor (0: from the open file limit, ADR 0039)")
		idemTTL   = flag.Duration("idempotency-ttl", 24*time.Hour, "how long finished run ids stay idempotency keys across restarts (negative disables)")
	)
	flag.Parse()

	var tier engine.Tier
	if err := tier.UnmarshalText([]byte(*tierName)); err != nil {
		log.Fatal(err)
	}
	reg := ir.NewRegistry()
	// Built-in HTTP actions: an LLM call is unprotected (safe to re-run);
	// a generic request is left undeclared, so it is treated as real.
	reg.Register(ir.NodeSpec{Action: "http.llm", Effect: ir.EffectUnprotected, MaxAttempts: 4, Timeout: ir.Duration(10 * time.Minute)})
	if *nodesFile != "" {
		b, err := os.ReadFile(*nodesFile)
		if err != nil {
			log.Fatal(err)
		}
		if err := reg.LoadJSON(b); err != nil {
			log.Fatalf("%s: %v", *nodesFile, err)
		}
	}
	limits := map[string]sched.DestLimits{}
	if *limitFile != "" {
		b, err := os.ReadFile(*limitFile)
		if err != nil {
			log.Fatal(err)
		}
		if err := json.Unmarshal(b, &limits); err != nil {
			log.Fatalf("%s: %v", *limitFile, err)
		}
	}
	var sink obs.Sink = obs.Discard
	switch *obsFile {
	case "":
	case "-":
		sink = obs.NewJSONLines(os.Stdout)
	default:
		f, err := os.OpenFile(*obsFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			log.Fatal(err)
		}
		defer f.Close()
		sink = obs.NewJSONLines(f)
	}

	e, err := engine.New(engine.Config{
		Shards:         *shards,
		Registry:       reg,
		DataDir:        *dataDir,
		NoSync:         *noSync,
		DefaultTier:    tier,
		IdempotencyTTL: *idemTTL,
		Admission: sched.AdmissionConfig{
			MaxActive: *maxActive, MaxQueued: *maxQueued,
			MaxActivePerTenant: func(string) int { return *perTenant },
		},
		Limits:  func(dest string) sched.DestLimits { return limits[dest] },
		Observe: sink,
	})
	if err != nil {
		log.Fatal(err)
	}
	if err := api.Load(e, *dataDir); err != nil {
		log.Fatal(err)
	}
	// The built-in HTTP executor serves every action named "http.*".
	hx := httpexec.New()
	var hmu sync.Mutex
	served := map[string]bool{}
	serveHTTP := func(action string) {
		hmu.Lock()
		defer hmu.Unlock()
		if strings.HasPrefix(action, "http.") && !served[action] {
			served[action] = true
			e.RegisterExecutor([]string{action}, *httpConc, hx)
		}
	}
	serveHTTP("http.llm")
	serveHTTP("http.request")
	for _, s := range reg.Specs() {
		serveHTTP(s.Action)
	}
	if err := e.Start(); err != nil {
		log.Fatal(err)
	}

	ws := &protocol.Server{E: e}
	if *sockPath == "" {
		*sockPath = filepath.Join(*dataDir, "worker.sock")
	}
	os.Remove(*sockPath)
	ul, err := net.Listen("unix", *sockPath)
	if err != nil {
		log.Fatal(err)
	}
	go ws.Serve(ul)
	if *tcpAddr != "" {
		tl, err := net.Listen("tcp", *tcpAddr)
		if err != nil {
			log.Fatal(err)
		}
		go ws.Serve(tl)
	}

	srv := &http.Server{Addr: *httpAddr, Handler: (&api.API{E: e, Dir: *dataDir, OnNode: func(s ir.NodeSpec) { serveHTTP(s.Action) }}).Handler()}
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatal(err)
		}
	}()
	log.Printf("kairod: api on http://%s, workers on unix:%s, data in %s", *httpAddr, *sockPath, *dataDir)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	log.Printf("kairod: shutting down")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	srv.Shutdown(ctx)
	ul.Close()
	e.Close()
}
