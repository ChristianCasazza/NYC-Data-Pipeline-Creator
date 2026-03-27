.PHONY: install dev sync notebook venv venv-win

install:
	@if [ ! -d .venv ]; then uv venv; fi
	uv sync --extra notebook
	@mkdir -p logs data/landing data/clean data/exports
	@if [ ! -f .env ]; then cp .env.example .env && echo "Created .env from .env.example — edit it with your API keys"; else echo ".env already exists"; fi
	@echo ""
	@echo "Setup complete. Activate venv and run:"
	@echo "  source .venv/bin/activate"
	@echo "  make dev"

venv:
	@if [ ! -d .venv ]; then uv venv; fi
	@echo "Run: source .venv/bin/activate"

venv-win:
	@if not exist .venv (uv venv)
	@echo Run: .venv\Scripts\activate

dev:
	DAGSTER_HOME=$(PWD)/logs uv run dg dev

sync:
	uv sync

notebook:
	uv sync --extra notebook
