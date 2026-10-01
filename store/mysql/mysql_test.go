package mysql

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"os"
	"strings"
	"testing"

	driver "github.com/go-sql-driver/mysql"

	"kairo/store/sqlstore/sqltest"
)

func registerTestCA(t *testing.T) {
	ca := os.Getenv("KAIRO_TEST_CA")
	if ca == "" {
		return
	}
	pem, err := os.ReadFile(ca)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(pem)
	driver.RegisterTLSConfig("kairo-test", &tls.Config{RootCAs: pool})
}

func TestMeasure(t *testing.T) {
	if dsn := os.Getenv("KAIRO_TEST_MYSQL_DSN"); dsn != "" {
		registerTestCA(t)
		db, err := Open(dsn, Options{})
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		t.Run("mysql", func(t *testing.T) { sqltest.Measure(t, db, MySQL) })
	}
	if dsn := os.Getenv("KAIRO_TEST_TIDB_DSN"); dsn != "" {
		db, err := Open(dsn, Options{AllowInsecureTransport: true})
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		t.Run("tidb", func(t *testing.T) { sqltest.Measure(t, db, TiDB) })
	}
}

func TestMySQL(t *testing.T) {
	dsn := os.Getenv("KAIRO_TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("KAIRO_TEST_MYSQL_DSN not set (scripts/check-backends.sh mysql)")
	}
	registerTestCA(t)
	db, err := Open(dsn, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Ping(); err != nil {
		t.Fatal(err)
	}
	// multiStatements is forced off: stacked statements are rejected.
	if _, err := db.Exec("SELECT 1; SELECT 2"); err == nil {
		t.Fatal("stacked statements were accepted")
	}
	sqltest.Run(t, db, MySQL)
}

func TestTiDB(t *testing.T) {
	dsn := os.Getenv("KAIRO_TEST_TIDB_DSN")
	if dsn == "" {
		t.Skip("KAIRO_TEST_TIDB_DSN not set (scripts/check-backends.sh tidb)")
	}
	// The test TiDB runs without TLS: allowed explicitly, for tests only.
	db, err := Open(dsn, Options{AllowInsecureTransport: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Ping(); err != nil {
		t.Fatal(err)
	}
	sqltest.Run(t, db, TiDB)
}

func TestRefusesInsecureTransport(t *testing.T) {
	for _, d := range []string{
		"u:SECRETPW@tcp(db.example:3306)/x",
		"u:SECRETPW@tcp(db.example:3306)/x?tls=false",
		"u:SECRETPW@tcp(db.example:3306)/x?tls=skip-verify",
		"u:SECRETPW@tcp(db.example:3306)/x?tls=preferred",
	} {
		if _, err := Open(d, Options{}); !errors.Is(err, ErrInsecure) {
			t.Errorf("%s: got %v, want ErrInsecure", d, err)
		}
	}
	db, err := Open("u:SECRETPW@tcp(db.example:3306)/x?tls=true&multiStatements=true&interpolateParams=true", Options{})
	if err != nil {
		t.Fatalf("tls=true refused: %v", err)
	}
	db.Close()
	_, err = Open("u:SECRETPW@tcp(db.example:3306)/x?tls=nosuchconfig", Options{})
	if err == nil || strings.Contains(err.Error(), "SECRETPW") {
		t.Fatalf("bad DSN error leaks or is missing: %v", err)
	}
}
