module kairo/store/postgres

go 1.26.4

toolchain go1.26.6

require (
	github.com/jackc/pgx/v5 v5.11.0
	kairo/store/sqlstore v0.0.0
)

require (
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	golang.org/x/sync v0.23.0 // indirect
	golang.org/x/text v0.42.0 // indirect
	kairo v0.0.0 // indirect
)

replace (
	kairo => ../..
	kairo/store/sqlstore => ../sqlstore
)
