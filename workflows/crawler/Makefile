.DEFAULT_GOAL := help

.PHONY: lint
lint: ## Run Linter
	uv run ruff check .
	uv run mypy .

.PHONY: fmt
fmt: ## Run formatter
	uv run ruff check --fix .
	uv run ruff format .

.PHONY: test
test: ## Run tests
	uv run pytest .

.PHONY: lock
lock: ## Lock dependencies
	uv lock

.PHONY: install
install: ## Setup the project
	uv sync --all-groups

.PHONY: help
help: ## Show options
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'
