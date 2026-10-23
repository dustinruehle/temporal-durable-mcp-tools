# One terminal per process, so you can stop any one of them on stage. Or `make up` to start everything
# in the background (logs in logs/) and `make down` to stop what `make up` started.

# Settings from .env reach every process started by make: Python, Java and TypeScript.
-include .env
export

.PHONY: setup server heritage-py heritage-java tool-worker mcp-server platform dashboard stop-tool-worker up down status test lint

setup:            ## Install Python, Node and Java dependencies
	uv sync --group dev
	cd mcp-server && npm install
	cd heritage/java && ./gradlew -q installDist

server:           ## Temporal dev server (gRPC 7233, Web UI 8233)
	temporal server start-dev --db-filename .run/temporal.db

heritage-py:      ## Mock heritage services: records, policy, notify (Python, 9101)
	uv run python -m heritage_py.app

heritage-java:    ## Mock heritage ledger (Java, 9103) plus its Temporal Worker on heritage-ledger
	cd heritage/java && ./gradlew -q installDist && exec build/install/heritage-java/bin/heritage-java

tool-worker:      ## Temporal Worker: FulfillRequest Workflow and the Python Activities
	uv run python -m durable_tools.worker

mcp-server:       ## MCP tool interface (TypeScript, 9200)
	cd mcp-server && npm start

platform:         ## Agent platform: plain LangGraph (Python, 9300)
	uv run python -m agent_platform.app

dashboard:        ## Demo control panel (8095)
	uv run python -m demo_dashboard.server

stop-tool-worker: ## Stop the tool Worker that `make up` started (beat 2k). `make up` starts it again
	kill -INT -- -$$(cat .run/tool-worker.pid) && rm -f .run/tool-worker.pid

up:               ## Start everything in the background
	./scripts/up.sh

down:             ## Stop what `make up` started
	./scripts/down.sh

status:           ## Show which processes answer
	./scripts/status.sh

test:             ## Python Workflow tests, TypeScript typecheck, Java tests
	uv run pytest -q
	cd mcp-server && npm run build
	cd heritage/java && ./gradlew -q test

lint:
	uv run ruff check heritage/python tools agent-platform dashboard tests scripts
