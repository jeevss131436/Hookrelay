# HookRelay developer tasks. Run `make help` for the list.
# On Windows, run make from Git Bash (recipes use POSIX sh).

# Load .env if present and export it to recipes (go run, go test).
-include .env
# `neon env pull` / `neon deploy` write quoted values (KEY="value"), but make's
# include keeps quotes literally. Strip one surrounding pair. Variables given
# on the command line (e.g. `make ci TEST_DATABASE_URL=`) still take priority.
ENV_QUOTED_VARS := $(shell [ -f .env ] && sed -n 's/^\([A-Za-z_][A-Za-z0-9_]*\)="[^"]*"[[:space:]]*$$/\1/p' .env)
$(foreach v,$(ENV_QUOTED_VARS),$(eval $(v) := $(patsubst "%",%,$(value $(v)))))
export

BACKEND_DIR         := backend
BIN_DIR             := bin
GOLANGCI_LINT_VERSION := v2.15.0
GOLANGCI_LINT       ?= golangci-lint
SQLC_VERSION        := v1.31.1
SQLC                ?= sqlc
GO_TEST_FLAGS       ?=
HOOKRELAY           := cd $(BACKEND_DIR) && go run ./cmd/hookrelay

.DEFAULT_GOAL := help

.PHONY: help
help: ## Show this help
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-14s %s\n", $$1, $$2}'

## ---- Local infrastructure ----

.PHONY: env
env: ## Create .env from .env.example (with a fresh random encryption key) if it does not exist
	@if [ -f .env ]; then echo ".env already exists"; else \
		cp .env.example .env && \
		key="$$(openssl rand -base64 32)" && \
		sed -i "s|^HOOKRELAY_ENCRYPTION_KEY=.*|HOOKRELAY_ENCRYPTION_KEY=$$key|" .env && \
		echo "created .env with a random HOOKRELAY_ENCRYPTION_KEY; edit the placeholder passwords"; fi

.PHONY: dev-up
dev-up: ## Start Postgres and Redis and wait until healthy
	docker compose up -d --wait

.PHONY: dev-down
dev-down: ## Stop Postgres and Redis (keeps data volume)
	docker compose down

.PHONY: dev-reset
dev-reset: ## Stop services and DELETE the Postgres data volume
	docker compose down -v

.PHONY: dev-ps
dev-ps: ## Show service status and health
	docker compose ps

## ---- Go ----

.PHONY: build
build: ## Build the hookrelay binary into bin/
	cd $(BACKEND_DIR) && go build -o ../$(BIN_DIR)/hookrelay$$(go env GOEXE) ./cmd/hookrelay

.PHONY: run-api
run-api: ## Run the API server (uses .env)
	cd $(BACKEND_DIR) && go run ./cmd/hookrelay api

.PHONY: run-worker
run-worker: ## Run the worker (placeholder until Phase 3)
	cd $(BACKEND_DIR) && go run ./cmd/hookrelay worker

.PHONY: fmt
fmt: ## Format Go code
	cd $(BACKEND_DIR) && gofmt -w .

.PHONY: fmt-check
fmt-check: ## Fail if any Go file is not gofmt-formatted
	@cd $(BACKEND_DIR) && unformatted="$$(gofmt -l .)"; \
	if [ -n "$$unformatted" ]; then echo "gofmt needed on:"; echo "$$unformatted"; exit 1; fi; \
	echo "gofmt: ok"

.PHONY: vet
vet: ## Run go vet
	cd $(BACKEND_DIR) && go vet ./...

.PHONY: lint
lint: ## Run golangci-lint (install with `make tools`)
	cd $(BACKEND_DIR) && $(GOLANGCI_LINT) run ./...

.PHONY: test
test: ## Run tests (integration tests run when TEST_DATABASE_URL/TEST_REDIS_URL are set)
	cd $(BACKEND_DIR) && go test $(GO_TEST_FLAGS) ./...

.PHONY: tidy
tidy: ## Tidy go.mod/go.sum
	cd $(BACKEND_DIR) && go mod tidy

.PHONY: tools
tools: ## Install pinned dev tools (golangci-lint, sqlc; sqlc needs cgo, see CLAUDE.md for Windows)
	go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)
	go install github.com/sqlc-dev/sqlc/cmd/sqlc@$(SQLC_VERSION)

.PHONY: ci
ci: fmt-check vet lint sqlc-check test ## Run the same checks as CI

## ---- Database ----

.PHONY: sqlc
sqlc: ## Regenerate Go code from db/queries (after editing queries or migrations)
	cd $(BACKEND_DIR) && $(SQLC) generate

.PHONY: sqlc-check
sqlc-check: ## Fail if generated sqlc code is out of date, or queries fail sqlc vet
	cd $(BACKEND_DIR) && $(SQLC) diff && $(SQLC) vet
	@echo "sqlc: up to date"

.PHONY: migrate
migrate: ## Apply pending migrations (uses DATABASE_URL_UNPOOLED, else DATABASE_URL)
	$(HOOKRELAY) migrate up

.PHONY: migrate-down
migrate-down: ## Roll back the most recent migration
	$(HOOKRELAY) migrate down

.PHONY: migrate-status
migrate-status: ## Show migration status
	$(HOOKRELAY) migrate status

.PHONY: create-project
create-project: ## Create a project + first API key: make create-project NAME="Acme"
	@if [ -z "$(NAME)" ]; then echo 'usage: make create-project NAME="Acme"'; exit 2; fi
	$(HOOKRELAY) admin create-project --name "$(NAME)"
