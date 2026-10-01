.PHONY: check quick race bench fmt build hooks

check:            ## fmt + vet + build + tests + invariants
	scripts/check.sh
quick:            ## what the hooks run
	scripts/check.sh --quick
race:             ## check + race detector
	scripts/check.sh --race
bench:            ## non-functional budgets (fails when over)
	scripts/bench.sh
fmt:
	gofmt -w .
build:
	go build -o bin/kairod ./cmd/kairod
hooks:            ## use the repository's git hooks
	git config core.hooksPath scripts/githooks
