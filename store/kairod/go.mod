module github.com/r-hashi01/kairo/store/kairod

go 1.27.4

require (
	github.com/r-hashi01/kairo v0.5.2
	github.com/r-hashi01/kairo/store/mysql v0.5.2
	github.com/r-hashi01/kairo/store/oracle v0.5.2
	github.com/r-hashi01/kairo/store/postgres v0.5.2
	github.com/r-hashi01/kairo/store/sqlite v0.5.2
	github.com/r-hashi01/kairo/store/sqlstore v0.5.2
)

require (
	filippo.io/edwards25519 v1.2.0 // indirect
	github.com/dustin/go-humanize v1.0.1 // indirect
	github.com/go-sql-driver/mysql v1.10.1 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/pgx/v5 v5.11.0 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	github.com/mattn/go-isatty v0.0.24 // indirect
	github.com/ncruces/go-strftime v1.0.0 // indirect
	github.com/remyoudompheng/bigfft v0.0.0-20230129092748-24d4a6f8daec // indirect
	github.com/sijms/go-ora/v2 v2.9.0 // indirect
	golang.org/x/sync v0.23.0 // indirect
	golang.org/x/sys v0.48.0 // indirect
	golang.org/x/text v0.42.0 // indirect
	modernc.org/libc v1.77.1 // indirect
	modernc.org/mathutil v1.7.1 // indirect
	modernc.org/memory v1.12.1 // indirect
	modernc.org/sqlite v1.60.1 // indirect
)

replace (
	github.com/r-hashi01/kairo => ../..
	github.com/r-hashi01/kairo/store/mysql => ../mysql
	github.com/r-hashi01/kairo/store/oracle => ../oracle
	github.com/r-hashi01/kairo/store/postgres => ../postgres
	github.com/r-hashi01/kairo/store/sqlite => ../sqlite
	github.com/r-hashi01/kairo/store/sqlstore => ../sqlstore
)
