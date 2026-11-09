package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	kairo "github.com/r-hashi01/kairo/sdk/go"
)

const (
	actionName   = "qualify.restart.once"
	workflowName = "qualify.restart"
	runID        = "qualify-restart-1"
)

type daemon struct {
	cmd *exec.Cmd
}

func required(name string) string {
	v := os.Getenv(name)
	if v == "" {
		panic(name + " is required")
	}
	return v
}

func freePort() int {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

func startDaemon(ctx context.Context, binary, data, socket, url, logPath string) (*daemon, error) {
	addr := strings.TrimPrefix(url, "http://")
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	cmd := exec.Command(binary, "-data", data, "-http", addr, "-socket", socket, "-tier", "file")
	cmd.Stdout, cmd.Stderr = logFile, logFile
	if err := cmd.Start(); err != nil {
		logFile.Close()
		return nil, err
	}
	logFile.Close()
	client := &http.Client{Timeout: 100 * time.Millisecond}
	deadline := time.Now().Add(10 * time.Second)
	for {
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, url+"/v1/stats", nil)
		if res, err := client.Do(req); err == nil {
			res.Body.Close()
			if res.StatusCode == http.StatusOK {
				return &daemon{cmd: cmd}, nil
			}
		}
		if time.Now().After(deadline) {
			cmd.Process.Kill()
			cmd.Wait()
			return nil, errors.New("kairod did not start")
		}
		select {
		case <-ctx.Done():
			cmd.Process.Kill()
			cmd.Wait()
			return nil, ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func (d *daemon) kill() error {
	if err := d.cmd.Process.Kill(); err != nil {
		return err
	}
	return d.cmd.Wait()
}

func (d *daemon) stop() {
	_ = d.cmd.Process.Signal(syscall.SIGTERM)
	_ = d.cmd.Wait()
}

func openKairo(ctx context.Context, url, socket, marker string) (*kairo.Kairo, error) {
	k, err := kairo.Connect(ctx, url, socket, kairo.Options{Mode: kairo.Suspend, Lease: 300 * time.Millisecond, Concurrency: 8, Wake: func(time.Time) error { return nil }})
	if err != nil {
		return nil, err
	}
	kairo.Action(k, actionName, kairo.Real, func(_ *kairo.TaskContext, value string) (string, error) {
		f, err := os.OpenFile(marker, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
		if errors.Is(err, os.ErrExist) {
			return "", errors.New("real action ran twice")
		}
		if err != nil {
			return "", err
		}
		if _, err := f.WriteString(value + "\n"); err != nil {
			f.Close()
			return "", err
		}
		return value, f.Close()
	})
	kairo.Workflow(k, workflowName, func(c *kairo.Context, value string) (string, error) {
		out, err := kairo.Call[string](c, actionName, value)
		if err != nil {
			return "", err
		}
		if err := c.Sleep(2 * time.Second); err != nil {
			return "", err
		}
		by, err := kairo.WaitFor[string](c, "continue")
		return out + ":" + by, err
	}, kairo.WorkflowVersion("qualification-v1"))
	if err := k.Start(ctx); err != nil {
		k.Close()
		return nil, err
	}
	return k, nil
}

func waitForSignal(ctx context.Context, k *kairo.Kairo) error {
	for {
		if _, _, err := k.Tick(ctx); err != nil {
			return err
		}
		children, err := k.Children(ctx, runID)
		if err != nil {
			return err
		}
		for _, child := range children {
			if strings.HasPrefix(child.Plan, "kairo.wait/continue") && !child.Finished() {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(5 * time.Millisecond):
		}
	}
}

func main() {
	binary, work := required("KAIRO_FAULT_KAIROD"), required("KAIRO_FAULT_WORK")
	data, socket := filepath.Join(work, "data"), filepath.Join(work, "worker.sock")
	marker, logPath := filepath.Join(work, "real-action.marker"), filepath.Join(work, "kairod.log")
	url := fmt.Sprintf("http://127.0.0.1:%d", freePort())
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	d1, err := startDaemon(ctx, binary, data, socket, url, logPath)
	if err != nil {
		panic(err)
	}
	k1, err := openKairo(ctx, url, socket, marker)
	if err != nil {
		d1.stop()
		panic(err)
	}
	if _, err := kairo.Run[string](ctx, k1, workflowName, "effect", kairo.WithID(runID)); !errors.Is(err, kairo.ErrSuspended) {
		k1.Close()
		d1.stop()
		panic(fmt.Sprintf("first run: %v", err))
	}
	if _, err := os.Stat(marker); err != nil {
		k1.Close()
		d1.stop()
		panic(fmt.Sprintf("real action was not recorded before kill: %v", err))
	}
	if err := d1.kill(); err == nil {
		panic("SIGKILL unexpectedly reported success")
	}
	k1.Close()
	time.Sleep(2100 * time.Millisecond) // let the durable timer become due while kairod is down

	d2, err := startDaemon(ctx, binary, data, socket, url, logPath)
	if err != nil {
		panic(err)
	}
	defer d2.stop()
	k2, err := openKairo(ctx, url, socket, marker)
	if err != nil {
		panic(err)
	}
	defer k2.Close()
	if err := waitForSignal(ctx, k2); err != nil {
		panic(err)
	}
	if err := k2.Signal(ctx, runID, "continue", "resumed"); err != nil {
		panic(err)
	}
	out, err := kairo.Await[string](ctx, k2, runID)
	if err != nil || out != "effect:resumed" {
		panic(fmt.Sprintf("result %q: %v", out, err))
	}
	b, err := os.ReadFile(marker)
	if err != nil || string(b) != "effect\n" {
		panic(fmt.Sprintf("real action marker %q: %v", b, err))
	}
	fmt.Println("PASS: SIGKILL during timer wait recovered; real action executed once")
}
