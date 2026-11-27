package storetest

import (
	"bufio"
	"context"
	"database/sql"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	kairo "github.com/r-hashi01/kairo/sdk/go"
)

// TestMain lets this test binary hold an owner's mark in a process of its
// own (KAIRO_TEST_HOLD), to be killed.
func TestMain(m *testing.M) {
	if path, owner := os.Getenv("KAIRO_TEST_HOLD_DB"), os.Getenv("KAIRO_TEST_HOLD_OWNER"); path != "" {
		hold(path, owner)
		return
	}
	os.Exit(m.Run())
}

func hold(path, owner string) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		panic(err)
	}
	s, err := kairo.NewSQLStore(db, kairo.SQLite)
	if err != nil {
		panic(err)
	}
	if err := s.Liveness().Hold(context.Background(), owner); err != nil {
		panic(err)
	}
	fmt.Println("held")
	select {} // until killed
}

// SQLite: an owner's mark is held while its process lives, and gone once
// the process is killed, the OS taking its lock away (ADR 0086). An owner
// that never held one is not taken for gone.
func TestSQLiteOwnerMarkGoesWithItsProcess(t *testing.T) {
	path := filepath.Join(t.TempDir(), "k.db")
	s := openSQLite(t, path).(*kairo.SQLStore)
	l := s.Liveness()
	if l == nil {
		t.Fatal("no liveness for a SQLite file")
	}
	ctx := context.Background()
	if l.Gone(ctx, "never") {
		t.Fatal("an owner that never held a mark taken for gone")
	}
	cmd := exec.Command(os.Args[0], "-test.run=^$")
	cmd.Env = append(os.Environ(), "KAIRO_TEST_HOLD_DB="+path, "KAIRO_TEST_HOLD_OWNER=other")
	out, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer cmd.Process.Kill()
	if line, _ := bufio.NewReader(out).ReadString('\n'); line != "held\n" {
		t.Fatalf("the other process: %q", line)
	}
	if l.Gone(ctx, "other") {
		t.Fatal("a live owner taken for gone")
	}
	if err := cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = cmd.Wait()
	deadline := time.Now().Add(10 * time.Second)
	for !l.Gone(ctx, "other") {
		if time.Now().After(deadline) {
			t.Fatal("a killed owner's mark is still there")
		}
		time.Sleep(20 * time.Millisecond)
	}
}
