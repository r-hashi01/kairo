module github.com/r-hashi01/kairo/compat/n8n

go 1.27.2

require (
	github.com/r-hashi01/kairo v0.3.0
	github.com/r-hashi01/kairo/store/postgres v0.3.0
)

require (
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/pgx/v5 v5.11.0 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	golang.org/x/sync v0.23.0 // indirect
	golang.org/x/text v0.42.0 // indirect
	github.com/r-hashi01/kairo/store/sqlstore v0.3.0 // indirect
)

replace (
	github.com/r-hashi01/kairo => ../..
	github.com/r-hashi01/kairo/store/postgres => ../../store/postgres
	github.com/r-hashi01/kairo/store/sqlstore => ../../store/sqlstore
)
