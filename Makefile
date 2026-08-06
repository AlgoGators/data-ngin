# data-ngin developer entrypoints.
#
# Fresh clone, first time:
#     make install        # dependencies + .env scaffold
#     make test           # run the suite
#     make dag-check      # verify the Airflow DAGs parse
SHELL := /bin/bash

POETRY ?= poetry

.DEFAULT_GOAL := help
.PHONY: help install test dag-check lock clean

help: ## Show available targets
	@echo "data-ngin -- Airflow market-data ETL pipeline"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

install: ## Install dependencies and scaffold .env (main entrypoint)
	@command -v $(POETRY) >/dev/null 2>&1 || { \
		echo "poetry not found. Install it: https://python-poetry.org/docs/#installation"; \
		exit 1; \
	}
	@$(POETRY) install
	@if [ ! -f .env ] && [ -f .env.template ]; then \
		cp .env.template .env; \
		echo ""; \
		echo "Created .env from .env.template -- fill in your credentials before running the pipeline."; \
	fi
	@echo ""
	@echo "Ready. Run 'make test' or 'make dag-check'."

test: ## Run the test suite
	@$(POETRY) run pytest -q

dag-check: ## Verify every Airflow DAG parses (mirrors the CI gate)
	@AIRFLOW__CORE__LOAD_EXAMPLES=False $(POETRY) run python -c "\
	from airflow.models.dagbag import DagBag; \
	bag = DagBag(dag_folder='dags', include_examples=False); \
	[print(f'ERROR {p}: {e}') for p, e in bag.import_errors.items()]; \
	exit(1) if bag.import_errors else print(f'OK: {len(bag.dags)} DAG(s) parsed cleanly')"

lock: ## Verify poetry.lock is in sync with pyproject.toml
	@$(POETRY) check --lock

clean: ## Remove caches and build artifacts
	@find . -type d -name __pycache__ -prune -exec rm -rf {} + 2>/dev/null || true
	@rm -rf .pytest_cache
	@echo "Cleaned caches."
