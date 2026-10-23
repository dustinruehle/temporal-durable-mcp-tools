#!/usr/bin/env bash
# Stop the processes `make up` started, by PID (process group), newest first.
cd "$(dirname "$0")/.."
for name in dashboard platform mcp-server tool-worker heritage-java heritage-py server; do
  f=.run/$name.pid
  [[ -f $f ]] || continue
  pid=$(cat "$f")
  if kill -0 "$pid" 2>/dev/null; then
    kill -INT -- "-$pid" 2>/dev/null || kill -INT "$pid" 2>/dev/null
    for _ in $(seq 1 20); do kill -0 "$pid" 2>/dev/null || break; sleep 0.3; done
    kill -0 "$pid" 2>/dev/null && { kill -KILL -- "-$pid" 2>/dev/null || kill -KILL "$pid" 2>/dev/null; }
    echo "  stopped $name ($pid)"
  fi
  rm -f "$f"
done
