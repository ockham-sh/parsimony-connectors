# parsimony-connectors developer workbench.
#
# The sweep loop per package is: ruff → mypy → pytest → strict plugin listing.
# This Makefile mirrors CI exactly so "green locally" means "green in CI".
#
# Usage:
#   make verify PKG=treasury         # four-gate check on one package
#   make verify-all                  # four-gate check across every package
#   make sync                        # resolve the workspace (no gates)
#
# The PKG argument must match a directory under packages/.

.PHONY: help sync verify verify-all readme-roster clean

PKG ?=

help:
	@echo "Targets:"
	@echo "  sync                  — uv sync --all-extras --all-packages"
	@echo "  verify PKG=<name>     — ruff + mypy + pytest + strict plugin listing"
	@echo "  verify-all            — verify across every package under packages/*"
	@echo "  readme-roster         — regenerate the connector roster table in README.md"
	@echo "  clean                 — wipe caches (.pytest_cache, .mypy_cache, .ruff_cache)"

sync:
	uv sync --all-extras --all-packages

verify:
	@if [ -z "$(PKG)" ]; then \
		echo "error: PKG is required, e.g. 'make verify PKG=treasury'"; \
		exit 2; \
	fi
	@if [ ! -d "packages/$(PKG)" ]; then \
		echo "error: packages/$(PKG) does not exist"; \
		exit 2; \
	fi
	@set -e; \
	echo "==> ruff $(PKG)"; \
	uv run ruff check "packages/$(PKG)"; \
	echo "==> mypy $(PKG)"; \
	uv run mypy "packages/$(PKG)"; \
	echo "==> pytest $(PKG)"; \
	uv run pytest "packages/$(PKG)"; \
	echo "==> strict plugin listing"; \
	uv run parsimony list --strict

verify-all:
	@set -e; \
	for d in packages/*/; do \
		pkg=$$(basename "$$d"); \
		$(MAKE) --no-print-directory verify PKG=$$pkg; \
	done

readme-roster:
	uv run python scripts/gen_roster.py --update-readme

clean:
	find . -type d \( -name .pytest_cache -o -name .mypy_cache -o -name .ruff_cache -o -name __pycache__ \) -prune -exec rm -rf {} +
