package oracle

import (
	"errors"
	"os"
	"strings"
	"testing"

	"kairo/store/sqlstore/sqltest"
)

func TestMeasure(t *testing.T) {
	dsn := os.Getenv("KAIRO_TEST_ORACLE_DSN")
	if dsn == "" {
		t.Skip("KAIRO_TEST_ORACLE_DSN not set")
	}
	db, err := Open(dsn, Options{AllowInsecureTransport: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	sqltest.Measure(t, db, Dialect)
}

func TestOracle(t *testing.T) {
	dsn := os.Getenv("KAIRO_TEST_ORACLE_DSN")
	if dsn == "" {
		t.Skip("KAIRO_TEST_ORACLE_DSN not set (scripts/check-backends.sh oracle)")
	}
	// The test Oracle runs without TCPS: allowed explicitly, for tests only.
	db, err := Open(dsn, Options{AllowInsecureTransport: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Ping(); err != nil {
		t.Fatal(err)
	}
	sqltest.Run(t, db, Dialect)
}

func TestRefusesInsecureTransport(t *testing.T) {
	for _, d := range []string{
		"oracle://u:SECRETPW@db.example:1521/svc",
		"oracle://u:SECRETPW@db.example:2484/svc?SSL=true&SSL%20VERIFY=false",
	} {
		if _, err := Open(d, Options{}); !errors.Is(err, ErrInsecure) {
			t.Errorf("%s: got %v, want ErrInsecure", d, err)
		}
	}
	if db, err := Open("oracle://u:SECRETPW@db.example:2484/svc?SSL=true", Options{}); err != nil {
		t.Errorf("SSL=true refused: %v", err)
	} else {
		db.Close()
	}
	_, err := Open("oracle://u:SECRETPW@db.example:notaport/svc", Options{})
	if err == nil || strings.Contains(err.Error(), "SECRETPW") {
		t.Fatalf("bad DSN error leaks or is missing: %v", err)
	}
}
