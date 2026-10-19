#!/usr/bin/env bash
#
# Smoke Test
# ==========
#
# Checks that the local stack works end to end:
# 1. kafka and postgres containers are healthy
# 2. the topic exists with the right number of partitions
# 3. a message can be sent through kafka and read back
# 4. postgres answers queries, and the containers can reach each other
#
# The test message is a throwaway "smoke-<timestamp>" string. The Spark job
# rejects it as not valid JSON, which is why the dashboard leaves dead
# letters starting with "smoke-" out of its table.
#
# Usage (from repo root):
#     bash scripts/smoke_test.sh
#
# Dependencies:
# - docker, with the stack from docker/docker-compose.yml running

set -u

# ── Config ───────────────────────────────────────────────────────────
# Load .env if it exists so the test uses the same names as the stack
if [ -f .env ]; then
  set -a; . ./.env; set +a
fi

TOPIC="${ALERTS_TOPIC:-weather-alerts}"
PARTITIONS="${ALERTS_TOPIC_PARTITIONS:-4}"
PG_USER="${POSTGRES_USER:-stormstream}"
PG_DB="${POSTGRES_DB:-stormstream}"
KAFKA_BIN=/opt/kafka/bin

# ── Helper ───────────────────────────────────────────────────────────
pass=0
fail=0
check() {
  # Usage: check "name" command...
  # Runs the command quietly and counts it as a pass if it exits 0
  local name="$1"; shift
  if "$@" > /dev/null 2>&1; then
    echo "  PASS  $name"; pass=$((pass+1))
  else
    echo "  FAIL  $name"; fail=$((fail+1))
  fi
}

# ── Containers ───────────────────────────────────────────────────────
echo "containers"
check "kafka is healthy"    bash -c '[ "$(docker inspect -f "{{.State.Health.Status}}" stormstream-kafka)" = "healthy" ]'
check "postgres is healthy" bash -c '[ "$(docker inspect -f "{{.State.Health.Status}}" stormstream-postgres)" = "healthy" ]'

# ── Kafka topic ──────────────────────────────────────────────────────
echo "kafka"
check "topic $TOPIC exists" docker exec stormstream-kafka $KAFKA_BIN/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic "$TOPIC"
check "topic has $PARTITIONS partitions" bash -c "docker exec stormstream-kafka $KAFKA_BIN/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic $TOPIC | grep -q 'PartitionCount: $PARTITIONS'"

# ── Round trip ───────────────────────────────────────────────────────
# Produce one keyed message and read it back through the INTERNAL listener
MSG="smoke-$(date +%s)"
echo "round trip"
check "produce message" bash -c "echo 'FL:$MSG' | docker exec -i stormstream-kafka $KAFKA_BIN/kafka-console-producer.sh --bootstrap-server kafka:29092 --topic $TOPIC --property parse.key=true --property key.separator=:"
check "consume message back" bash -c "docker exec stormstream-kafka $KAFKA_BIN/kafka-console-consumer.sh --bootstrap-server kafka:29092 --topic $TOPIC --from-beginning --timeout-ms 15000 | grep -q '$MSG'"

# ── Postgres and network ─────────────────────────────────────────────
echo "postgres"
check "psql SELECT 1" docker exec stormstream-postgres psql -U "$PG_USER" -d "$PG_DB" -c "SELECT 1;"
check "postgres reachable from kafka container (network)" docker exec stormstream-kafka bash -c "timeout 5 bash -c '</dev/tcp/postgres/5432'"
check "kafka reachable from postgres container (network)" docker exec stormstream-postgres bash -c "timeout 5 bash -c '</dev/tcp/kafka/29092'"

# ── Result ───────────────────────────────────────────────────────────
echo
echo "$pass passed, $fail failed"
[ "$fail" -eq 0 ]
