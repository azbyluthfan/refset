# Library targets run with GOWORK=off so the library module is checked on its own,
# exactly as CI does.
export GOWORK := off

GO ?= go
PKGS := ./...

.PHONY: all ci test race cover bench vet fmt fmt-check lint check-deps vulncheck fuzz soak examples pg-up pg-down demo demo-lru

all: ci

## ci: everything CI runs for the library module
ci: fmt-check vet check-deps race lint

## test: run unit tests
test:
	$(GO) test $(PKGS)

## race: run tests with the race detector, in random order
race:
	$(GO) test -race -shuffle=on $(PKGS)

## cover: run tests with coverage and print a summary
cover:
	$(GO) test -race -coverprofile=coverage.out -covermode=atomic $(PKGS)
	$(GO) tool cover -func=coverage.out | tail -n 1

## bench: run benchmarks in the library module (comparisons live in bench/)
bench:
	$(GO) test -run='^$$' -bench=. -benchmem $(PKGS)

## vet: go vet
vet:
	$(GO) vet $(PKGS)

## fmt: format all Go files
fmt:
	gofmt -s -w .

## fmt-check: fail if any Go file is not gofmt'ed
fmt-check:
	@out="$$(gofmt -s -l .)"; if [ -n "$$out" ]; then echo "Not gofmt'ed:"; echo "$$out"; exit 1; fi

## lint: golangci-lint (https://golangci-lint.run); skipped with a warning if not installed
lint:
	@if command -v golangci-lint >/dev/null 2>&1; then golangci-lint run $(PKGS); \
	else echo "golangci-lint not installed; skipping (CI runs it)"; fi

## check-deps: fail if the library module has any dependency
check-deps:
	./scripts/check-zero-deps.sh

GOVULNCHECK_VERSION ?= v1.9.0

## vulncheck: govulncheck on all three modules (needs network to fetch the tool)
vulncheck:
	$(GO) run golang.org/x/vuln/cmd/govulncheck@$(GOVULNCHECK_VERSION) $(PKGS)
	cd examples && $(GO) run golang.org/x/vuln/cmd/govulncheck@$(GOVULNCHECK_VERSION) ./...
	cd bench && $(GO) run golang.org/x/vuln/cmd/govulncheck@$(GOVULNCHECK_VERSION) ./...

FUZZTIME ?= 30s

## fuzz: run every fuzz target in the library module for FUZZTIME each
fuzz:
	@set -e; for pkg in $$($(GO) list $(PKGS)); do \
		for target in $$($(GO) test -list '^Fuzz' $$pkg | grep '^Fuzz' || true); do \
			echo "== $$pkg $$target ($(FUZZTIME))"; \
			$(GO) test -run '^$$' -fuzz "^$$target$$" -fuzztime $(FUZZTIME) $$pkg; \
		done; \
	done

SOAK_DURATION ?= 10m

## soak: replicas under random faults for SOAK_DURATION, race detector on (log in soak.log)
soak:
	REFSET_SOAK=$(SOAK_DURATION) REFSET_SOAK_LOG=$(CURDIR)/soak.log \
		$(GO) test -race -count=1 -run '^TestSoak$$' -timeout 3h -v ./internal/scenario/

PG_URL ?= postgres://refset:refset@localhost:55432/refset?sslmode=disable
COMPOSE := docker compose -f examples/docker-compose.yml

## pg-up: start Postgres 17 for the examples (Docker)
pg-up:
	$(COMPOSE) up -d --wait

## pg-down: stop the examples' Postgres
pg-down:
	$(COMPOSE) down

## examples: vet and test the examples module against Postgres (starts it)
examples: pg-up
	cd examples && $(GO) vet ./... && REFSET_PG_URL='$(PG_URL)' $(GO) test -race ./...

## demo: the plan-upgrade story over real HTTP and Postgres, one command
demo: pg-up
	cd examples && REFSET_PG_URL='$(PG_URL)' $(GO) run ./plan-upgrade

## demo-lru: the same story with a TTL cache, no database needed
demo-lru:
	cd examples && $(GO) run ./baseline-lru
