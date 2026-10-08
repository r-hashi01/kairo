module kairo/compat/n8n

go 1.27.1

require (
	kairo v0.0.0
	kairo/store/postgres v0.0.0-00010101000000-000000000000
)

require (
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/pgx/v5 v5.11.0 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	golang.org/x/sync v0.23.0 // indirect
	golang.org/x/text v0.42.0 // indirect
	kairo/store/sqlstore v0.0.0 // indirect
)

replace (
	kairo => ../..
	kairo/store/postgres => ../../store/postgres
	kairo/store/sqlstore => ../../store/sqlstore
)
