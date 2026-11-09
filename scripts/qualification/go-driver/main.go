package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	kairo "github.com/r-hashi01/kairo/sdk/go"
)

const (
	actionName   = "qualify.go.once"
	workflowName = "qualify.go"
	version      = "qualification-v1"
)

func env(name string) string {
	v := os.Getenv(name)
	if v == "" {
		panic(name + " is required")
	}
	return v
}

func marker(dir, id string) string {
	return filepath.Join(dir, fmt.Sprintf("%x.done", sha256.Sum256([]byte(id))))
}

func waitBarrier(ctx context.Context, dir, label string) error {
	if dir == "" {
		return nil
	}
	if err := os.WriteFile(filepath.Join(dir, label+".done"), nil, 0o644); err != nil {
		return err
	}
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		if _, err := os.Stat(filepath.Join(dir, "release")); err == nil {
			return nil
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func runOne(ctx context.Context, k *kairo.Kairo, tick func() error, id string) error {
	if _, err := kairo.Run[string](ctx, k, workflowName, id, kairo.WithID(id)); !errors.Is(err, kairo.ErrSuspended) {
		return fmt.Errorf("initial run: %w", err)
	}
	for {
		if err := tick(); err != nil {
			return fmt.Errorf("tick: %w", err)
		}
		children, err := k.Children(ctx, id)
		if err != nil {
			return fmt.Errorf("children: %w", err)
		}
		waiting := false
		for _, child := range children {
			if strings.HasPrefix(child.Plan, "kairo.wait/continue") && !child.Finished() {
				waiting = true
				break
			}
		}
		if waiting {
			if err := k.Signal(ctx, id, "continue", id); err != nil {
				return fmt.Errorf("signal: %w", err)
			}
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(5 * time.Millisecond):
		}
	}
	for {
		out, err := kairo.Await[string](ctx, k, id)
		if err == nil {
			if out != id {
				return fmt.Errorf("result %q, want %q", out, id)
			}
			return nil
		}
		if !errors.Is(err, kairo.ErrSuspended) {
			return err
		}
		if err := tick(); err != nil {
			return err
		}
	}
}

func main() {
	url, socket, markers, prefix := env("KAIRO_QUALIFY_URL"), env("KAIRO_QUALIFY_SOCKET"), env("KAIRO_QUALIFY_MARKERS"), env("KAIRO_QUALIFY_PREFIX")
	barrier, label := os.Getenv("KAIRO_QUALIFY_BARRIER"), os.Getenv("KAIRO_QUALIFY_DRIVER")
	runs, err := strconv.Atoi(env("KAIRO_QUALIFY_RUNS"))
	if err != nil || runs < 1 {
		panic("KAIRO_QUALIFY_RUNS must be positive")
	}
	inflight, err := strconv.Atoi(env("KAIRO_QUALIFY_INFLIGHT"))
	if err != nil || inflight < 1 {
		panic("KAIRO_QUALIFY_INFLIGHT must be positive")
	}
	inflight = min(inflight, runs)
	timeoutSeconds, err := strconv.Atoi(env("KAIRO_QUALIFY_TIMEOUT_SECONDS"))
	if err != nil || timeoutSeconds < 1 {
		panic("KAIRO_QUALIFY_TIMEOUT_SECONDS must be positive")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeoutSeconds)*time.Second)
	defer cancel()
	var phaseMu sync.Mutex
	phase := func(v any) {
		phaseMu.Lock()
		defer phaseMu.Unlock()
		_ = json.NewEncoder(os.Stdout).Encode(v)
	}
	k, err := kairo.Connect(ctx, url, socket, kairo.Options{
		Mode: kairo.Suspend, Lease: time.Second, Concurrency: 64, Wake: func(time.Time) error { return nil },
		Observe: func(o kairo.Observation) {
			if o.Kind == kairo.ObsStepStarted || o.Kind == kairo.ObsStepFinished {
				phase(map[string]any{"phase": "worker", "kind": o.Kind, "at_unix_ms": o.At.UnixMilli(), "run_id": o.RunID,
					"step_id": o.StepID, "attempt": o.Attempt, "duration_ms": float64(o.Duration) / float64(time.Millisecond)})
			}
		},
	})
	if err != nil {
		panic(err)
	}
	defer k.Close()
	kairo.Action(k, actionName, kairo.Real, func(_ *kairo.TaskContext, id string) (string, error) {
		f, err := os.OpenFile(marker(markers, id), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
		if errors.Is(err, os.ErrExist) {
			return "", fmt.Errorf("duplicate real action for %s", id)
		}
		if err != nil {
			return "", err
		}
		if _, err := f.WriteString(id + "\n"); err != nil {
			f.Close()
			return "", err
		}
		return id, f.Close()
	})
	kairo.Workflow(k, workflowName, func(ctx *kairo.Context, id string) (string, error) {
		if _, err := kairo.Call[string](ctx, actionName, id); err != nil {
			return "", err
		}
		if err := ctx.Sleep(25 * time.Millisecond); err != nil {
			return "", err
		}
		return kairo.WaitFor[string](ctx, "continue")
	}, kairo.WorkflowVersion(version))
	if err := k.Start(ctx); err != nil {
		panic(err)
	}

	tick := func() error {
		began := time.Now()
		_, _, err := k.Tick(ctx)
		phase(map[string]any{"phase": "claim_drive", "duration_ms": float64(time.Since(began)) / float64(time.Millisecond)})
		return err
	}
	jobs := make(chan string)
	errs := make(chan error, runs)
	var wg sync.WaitGroup
	for range inflight {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for id := range jobs {
				errs <- runOne(ctx, k, tick, id)
			}
		}()
	}
	for i := range runs {
		jobs <- fmt.Sprintf("qualify-go-%s-%06d", prefix, i)
	}
	close(jobs)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			panic(err)
		}
	}
	if err := waitBarrier(ctx, barrier, label); err != nil {
		panic(err)
	}
	fmt.Printf("go driver %s: %d workflows completed\n", prefix, runs)
}
