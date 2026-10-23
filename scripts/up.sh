#!/usr/bin/env bash
# Start every process in the background. Logs go to logs/<name>.log, PIDs to .run/<name>.pid.
# Uses an already-running Temporal server on TEMPORAL_ADDRESS if one answers; otherwise starts a dev server.
set -euo pipefail
set -m  # each background job gets its own process group, so down.sh can stop the whole group
cd "$(dirname "$0")/.."
mkdir -p logs .run

start() {  # name, then the command
  local name=$1; shift
  if [[ -f .run/$name.pid ]] && kill -0 "$(cat .run/$name.pid)" 2>/dev/null; then
    echo "  $name already running (pid $(cat .run/$name.pid))"; return
  fi
  nohup "$@" > "logs/$name.log" 2>&1 &
  echo $! > ".run/$name.pid"
  echo "  $name started (pid $!), log: logs/$name.log"
}

wait_for() {  # name, url
  for _ in $(seq 1 60); do curl -fsS "$2" > /dev/null 2>&1 && return 0; sleep 1; done
  echo "  $1 did not answer at $2; see logs/$1.log"; return 1
}

addr=${TEMPORAL_ADDRESS:-localhost:7233}
if ! nc -z "${addr%:*}" "${addr##*:}" 2>/dev/null; then
  start server temporal server start-dev --db-filename .run/temporal.db --log-level warn
  for _ in $(seq 1 30); do nc -z localhost 7233 2>/dev/null && break; sleep 1; done
fi

start heritage-py uv run python -m heritage_py.app
(cd heritage/java && ./gradlew -q installDist)
start heritage-java heritage/java/build/install/heritage-java/bin/heritage-java
start tool-worker uv run python -m durable_tools.worker
start mcp-server bash -c "cd mcp-server && exec npm start --silent"
wait_for heritage-py http://localhost:9101/healthz
wait_for mcp-server http://localhost:9200/healthz
start platform uv run python -m agent_platform.app
start dashboard uv run python -m demo_dashboard.server
wait_for heritage-java http://localhost:9103/admin/stats
wait_for platform http://localhost:9300/healthz
wait_for dashboard http://localhost:8095/
echo
echo "Dashboard: http://localhost:8095   Temporal Web UI: ${TEMPORAL_UI_URL:-http://localhost:8233}"
