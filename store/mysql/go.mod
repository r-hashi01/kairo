module kairo/store/mysql

go 1.26.4

toolchain go1.26.6

require (
	github.com/go-sql-driver/mysql v1.10.1
	kairo/store/sqlstore v0.0.0
)

require (
	filippo.io/edwards25519 v1.2.0 // indirect
	kairo v0.0.0 // indirect
)

replace (
	kairo => ../..
	kairo/store/sqlstore => ../sqlstore
)
