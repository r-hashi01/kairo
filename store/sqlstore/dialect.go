package sqlstore

import (
	"fmt"
	"strings"
)

// Dialect holds what differs between SQL products. Product modules
// (store/postgres, store/mysql, ...) define one; nothing else in this
// package is product specific.
type Dialect struct {
	Name string
	// Placeholder returns the n-th (1-based) bind parameter: "?", "$1", ":1".
	Placeholder func(n int) string
	// Column types.
	IDType    string // log ids and object names (up to MaxKeyLen bytes)
	BigInt    string // LSNs, epochs, part numbers
	BytesType string // record and object payloads
	// ForUpdate is appended to the fencing SELECT to lock the meta row
	// (" FOR UPDATE"); empty where the database has a single writer.
	ForUpdate string
	// MultiRowInsert: INSERT ... VALUES (...), (...) is supported.
	MultiRowInsert bool
	// InsertRows, if set, builds a multi-row INSERT of rows rows into
	// table(cols) another way (Oracle before 23ai: INSERT ALL).
	InsertRows func(table, cols string, rows, ncols int, placeholder func(int) string) string
	// DeleteLimited returns a DELETE of at most n rows of table matching
	// where (a condition using bind parameters 1 and 2).
	DeleteLimited func(table, where string, n int) string
	// PartSize bounds one stored object row; larger objects are split.
	PartSize int
	// RetireBatch is how many log rows one DELETE removes.
	RetireBatch int
	// NoCreateIfNotExists: the product lacks CREATE TABLE IF NOT EXISTS
	// (Oracle before 23ai); a plain CREATE TABLE is issued and errors for
	// which TableExists reports true are ignored.
	NoCreateIfNotExists bool
	TableExists         func(error) bool
}

// Q is the placeholder style "?".
func Q(int) string { return "?" }

// Dollar is the placeholder style "$1".
func Dollar(n int) string { return fmt.Sprintf("$%d", n) }

// Colon is the placeholder style ":1".
func Colon(n int) string { return fmt.Sprintf(":%d", n) }

// SQLite is the dialect of SQLite (driver chosen by store/sqlite).
var SQLite = Dialect{
	Name: "sqlite", Placeholder: Q,
	IDType: "TEXT", BigInt: "INTEGER", BytesType: "BLOB",
	MultiRowInsert: true,
	DeleteLimited: func(t, w string, n int) string {
		return fmt.Sprintf("DELETE FROM %s WHERE rowid IN (SELECT rowid FROM %s WHERE %s LIMIT %d)", t, t, w, n)
	},
	PartSize: 1 << 20, RetireBatch: 10000,
}

// placeholders returns "(p1, p2, p3), (p4, ...)" for rows x cols.
func (d *Dialect) values(rows, cols, from int) string {
	var b strings.Builder
	n := from
	for r := 0; r < rows; r++ {
		if r > 0 {
			b.WriteString(", ")
		}
		b.WriteByte('(')
		for c := 0; c < cols; c++ {
			if c > 0 {
				b.WriteString(", ")
			}
			b.WriteString(d.Placeholder(n))
			n++
		}
		b.WriteByte(')')
	}
	return b.String()
}
