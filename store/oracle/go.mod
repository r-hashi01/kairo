module kairo/store/oracle

go 1.26.4

toolchain go1.26.6

require kairo/store/sqlstore v0.0.0

require (
	github.com/sijms/go-ora/v2 v2.9.0
	kairo v0.0.0 // indirect
)

replace (
	kairo => ../..
	kairo/store/sqlstore => ../sqlstore
)
