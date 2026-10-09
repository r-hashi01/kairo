module github.com/r-hashi01/kairo/store/oracle

go 1.27.2

require github.com/r-hashi01/kairo/store/sqlstore v0.2.0

require (
	github.com/sijms/go-ora/v2 v2.9.0
	github.com/r-hashi01/kairo v0.2.0 // indirect
)

replace (
	github.com/r-hashi01/kairo => ../..
	github.com/r-hashi01/kairo/store/sqlstore => ../sqlstore
)
