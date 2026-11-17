package main

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func TestWorkerTCPRequiresAuthenticationOrExplicitOptIn(t *testing.T) {
	tests := []struct {
		name          string
		tcpAddr       string
		token         string
		allowInsecure bool
		wantErr       bool
	}{
		{name: "unix only"},
		{name: "authenticated TCP", tcpAddr: ":8422", token: "secret"},
		{name: "explicit insecure TCP", tcpAddr: "127.0.0.1:8422", allowInsecure: true},
		{name: "unauthenticated TCP", tcpAddr: "127.0.0.1:8422", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateWorkerListener(tt.tcpAddr, tt.token, tt.allowInsecure)
			if (err != nil) != tt.wantErr {
				t.Fatalf("validateWorkerListener() = %v, want error %v", err, tt.wantErr)
			}
		})
	}
}

func TestUnixWorkerSocketIsOwnerOnly(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("UNIX socket permissions are not available")
	}
	dir, err := os.MkdirTemp("/tmp", "kairod-socket-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	path := filepath.Join(dir, "worker.sock")
	l, err := listenUnixWorker(path)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := info.Mode().Perm(); got != 0o600 {
		t.Fatalf("worker socket permissions = %04o, want 0600", got)
	}
}

func TestUnixWorkerSocketRefusesRegularFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "worker.sock")
	if err := os.WriteFile(path, []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}
	if l, err := listenUnixWorker(path); err == nil {
		l.Close()
		t.Fatal("regular file was replaced")
	}
	b, err := os.ReadFile(path)
	if err != nil || string(b) != "keep" {
		t.Fatalf("regular file changed: %q, %v", b, err)
	}
}
