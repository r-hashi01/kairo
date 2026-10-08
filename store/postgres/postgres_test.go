package postgres

import (
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/r-hashi01/kairo/store/sqlstore/sqltest"
)

func dsn(t *testing.T) string {
	d := os.Getenv("KAIRO_TEST_POSTGRES_DSN")
	if d == "" {
		t.Skip("KAIRO_TEST_POSTGRES_DSN not set (scripts/check-backends.sh postgres)")
	}
	return d
}

func TestMeasure(t *testing.T) {
	db, err := Open(dsn(t), Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	sqltest.Measure(t, db, Dialect)
}

func TestPostgres(t *testing.T) {
	db, err := Open(dsn(t), Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Ping(); err != nil {
		t.Fatal(err)
	}
	sqltest.Run(t, db, Dialect)
}

// Transports without verified TLS are refused unless explicitly allowed,
// and errors never echo the DSN (with its password).
func TestRefusesInsecureTransport(t *testing.T) {
	for _, d := range []string{
		"postgres://u:SECRETPW@db.example:5432/x?sslmode=disable",
		"postgres://u:SECRETPW@db.example:5432/x?sslmode=prefer",
		"postgres://u:SECRETPW@db.example:5432/x?sslmode=require",
		"postgres://u:SECRETPW@db.example:5432/x", // pgx default: prefer
	} {
		if _, err := Open(d, Options{}); !errors.Is(err, ErrInsecure) {
			t.Errorf("%s: got %v, want ErrInsecure", d[strings.Index(d, "?"):], err)
		}
	}
	if _, err := Open("postgres://u:SECRETPW@db.example:5432/x?sslmode=verify-full", Options{}); err != nil {
		t.Errorf("verify-full refused: %v", err)
	}
	if _, err := Open("postgres://u:SECRETPW@db.example:5432/x?sslmode=disable", Options{AllowInsecureTransport: true}); err != nil {
		t.Errorf("explicitly allowed insecure transport refused: %v", err)
	}
	_, err := Open("postgres://u:SECRETPW@db.example:5432/x?sslmode=bogus", Options{})
	if err == nil || strings.Contains(err.Error(), "SECRETPW") {
		t.Fatalf("bad DSN error leaks or is missing: %v", err)
	}
}
