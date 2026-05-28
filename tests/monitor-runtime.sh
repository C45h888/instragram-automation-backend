#!/bin/bash
# ============================================
# Runtime Observer / Monitor
# ============================================
# Watches the constitutional runtime over extended
# periods WITHOUT making changes. Records state
# drift, consumer lag growth, lineage accumulation,
# and projection consistency.
#
# This is NOT a test - it is pure observation.
# No assertions, no state mutations, no restarts.
#
# Usage:
#   ./tests/monitor-runtime.sh --duration=900 --interval=30
#   ./tests/monitor-runtime.sh --duration=600 --interval=15
# ============================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
COMPOSE_FILE="$PROJECT_ROOT/docker-compose.test.yml"

DURATION=${DURATION:-900}     # default 15 minutes
INTERVAL=${INTERVAL:-30}      # default 30 seconds
OUTPUT_DIR="$PROJECT_ROOT/tests/output/runtime-observations"
METRICS_FILE="$OUTPUT_DIR/monitor-$(date +%Y%m%d-%H%M%S).jsonl"

mkdir -p "$OUTPUT_DIR"

echo "=========================================="
echo " Runtime Observer — Continuous Monitoring"
echo "=========================================="
echo "  Duration:  ${DURATION}s ($(($DURATION / 60)) min)"
echo "  Interval:  ${INTERVAL}s"
echo "  Output:    $METRICS_FILE"
echo "  Redis:     localhost:6379"
echo ""

# Ensure Docker stack is running
echo "[Observer] Checking Docker stack..."
STACK_RUNNING=$(docker ps --filter "name=instagram-test-redis" --filter "status=running" -q 2>/dev/null | head -1)
if [ -z "$STACK_RUNNING" ]; then
  echo "[Observer] Docker stack not running. Starting..."
  docker-compose -f "$COMPOSE_FILE" up -d
  echo "[Observer] Waiting for services to be healthy..."
  sleep 8
else
  echo "[Observer] Docker stack already running."
fi

# Verify Redis is reachable
REDIS_UP=$(redis-cli -h localhost -p 6379 ping 2>/dev/null || echo "DOWN")
if [ "$REDIS_UP" != "PONG" ]; then
  echo "[Observer] ERROR: Redis not reachable at localhost:6379"
  echo "[Observer] Is the test stack running? Try: docker-compose -f $COMPOSE_FILE up -d"
  exit 1
fi
echo "[Observer] Redis: healthy"

# Redis keys we'll monitor
KEYS=(
  "governance:observability:projection"
  "lineage:ledger:entries"
  "lineage:worker:cursor"
  "lineage:projection:snapshot"
)

# Consumer names we expect from test phases
CONSUMERS=(
  "phase-2c-consumer"
  "phase-2d-consumer"
)

echo ""
echo "[Observer] Starting ${DURATION}s observation loop..."

start_time=$(date +%s)
iteration=0

while [ $(($(date +%s) - start_time)) -lt $DURATION ]; do
  iter_start=$(date +%s.%N)
  
  # Snapshot timestamp
  ts=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  
  # Collect state samples
  sample="{\"ts\":\"$ts\",\"iteration\":$iteration"
  
  # Redis key sizes (lineage growth indicator)
  for key in "${KEYS[@]}"; do
    size=$(redis-cli -h localhost -p 6379 dbsize 2>/dev/null || echo 0)
    # Get type of key to understand what's stored
    type=$(redis-cli -h localhost -p 6379 type "$key" 2>/dev/null || echo "none")
    sample="$sample,\"$key\":{\"size\":$size,\"type\":\"$type\"}"
  done
  
  # Consumer lag monitoring
  for consumer in "${CONSUMERS[@]}"; do
    # Consumer lag stored in observability projection via vitest
    # We'll check if consumer cursors are advancing
    cursor_key="test:consumer:cursor:$consumer"
    cursor_val=$(redis-cli -h localhost -p 6379 get "$cursor_key" 2>/dev/null || echo "null")
    sample="$sample,\"${consumer}_cursor\":$cursor_val"
  done
  
  # Projection log size (transition count) - handle WRONGTYPE errors
  proj_type=$(redis-cli -h localhost -p 6379 type "governance:observability:projection" 2>/dev/null || echo "none")
  if [ "$proj_type" = "list" ]; then
    proj_size=$(redis-cli -h localhost -p 6379 llen "governance:observability:projection" 2>/dev/null || echo 0)
  else
    proj_size="N/A"
  fi
  sample="$sample,\"projection_log_size\":$proj_size,\"projection_type\":\"$proj_type\""

  # Lineage ledger size
  ledger_type=$(redis-cli -h localhost -p 6379 type "lineage:ledger:entries" 2>/dev/null || echo "none")
  if [ "$ledger_type" = "list" ]; then
    ledger_size=$(redis-cli -h localhost -p 6379 llen "lineage:ledger:entries" 2>/dev/null || echo 0)
  else
    ledger_size="N/A"
  fi
  sample="$sample,\"lineage_ledger_size\":$ledger_size,\"ledger_type\":\"$ledger_type\""
  
  # Redis info metrics
  used_memory=$(redis-cli -h localhost -p 6379 info memory 2>/dev/null | grep "used_memory_human" | cut -d: -f2 | tr -d '\r\n')
  connected_clients=$(redis-cli -h localhost -p 6379 info clients 2>/dev/null | grep "connected_clients" | cut -d: -f2 | tr -d '\r\n')
  sample="$sample,\"used_memory\":\"$used_memory\",\"connected_clients\":$connected_clients"
  
  # Docker container health
  for container in test-redis test-postgres test-runner; do
    status=$(docker inspect --format='{{.State.Health.Status}}' instagram-$container 2>/dev/null || echo "unknown")
    running=$(docker inspect --format='{{.State.Running}}' instagram-$container 2>/dev/null || echo "false")
    sample="$sample,\"${container}_status\":\"$status\",\"${container}_running\":$running"
  done
  
  sample="$sample}"
  
  # Write to JSONL (append-only, for later analysis)
  echo "$sample" >> "$METRICS_FILE"
  
  iter_elapsed=$(echo "$(date +%s.%N) - $iter_start" | bc 2>/dev/null || echo "0")
  remaining=$(($DURATION - $(date +%s) + start_time))
  
  echo "[Observer] $(date +%H:%M:%S) | iter=$iteration | proj=$proj_size | ledger=$ledger_size | mem=$used_memory | remaining=${remaining}s"
  
  iteration=$((iteration + 1))
  
  # Sleep for interval minus elapsed iteration time (adaptive sampling)
  sleep_time=$(echo "$INTERVAL - $iter_elapsed" | bc 2>/dev/null || echo "$INTERVAL")
  sleep_time_int=${sleep_time%.*}  # truncate to integer
  if [ "$sleep_time_int" -gt 0 ] 2>/dev/null; then
    sleep $sleep_time_int
  fi
done

echo ""
echo "=========================================="
echo " Observation Complete"
echo "=========================================="
echo "  Iterations: $iteration"
echo "  Output:    $METRICS_FILE"
echo ""
echo "Key metrics to check:"
echo "  - projection_log_size growth should be monotonic"
echo "  - lineage_ledger_size growth should be monotonic"
echo "  - used_memory should be stable (no leak)"
echo "  - connected_clients should be stable"
echo "  - all containers should remain 'healthy' and 'running'"
echo ""
echo "Analyze with:"
echo "  cat $METRICS_FILE | jq '.projection_log_size' -s"
echo "  cat $METRICS_FILE | jq '.lineage_ledger_size' -s"
echo ""
echo "=========================================="
