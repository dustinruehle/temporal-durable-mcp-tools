#!/usr/bin/env bash
check() { if curl -fsS "$2" > /dev/null 2>&1; then echo "  up    $1  $2"; else echo "  DOWN  $1  $2"; fi; }
addr=${TEMPORAL_ADDRESS:-localhost:7233}
if nc -z "${addr%:*}" "${addr##*:}" 2>/dev/null; then echo "  up    temporal  $addr"; else echo "  DOWN  temporal  $addr"; fi
check heritage-py http://localhost:9101/healthz
check heritage-java http://localhost:9103/admin/stats
check mcp-server http://localhost:9200/healthz
check platform http://localhost:9300/healthz
check dashboard http://localhost:8095/
echo "  tool-worker and the Java Worker have no HTTP port: check logs/tool-worker.log and logs/heritage-java.log"
