module github.com/r-hashi01/kairo/store/mysql

go 1.27.4

require (
	github.com/go-sql-driver/mysql v1.10.1
	github.com/r-hashi01/kairo/store/sqlstore v0.5.2
)

require (
	filippo.io/edwards25519 v1.2.0 // indirect
	github.com/r-hashi01/kairo v0.5.2 // indirect
)

replace (
	github.com/r-hashi01/kairo => ../..
	github.com/r-hashi01/kairo/store/sqlstore => ../sqlstore
)
