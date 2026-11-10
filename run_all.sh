#!/usr/bin/env bash
# اجرای کل پروژه از صفر: ./run_all.sh
set -euo pipefail
cd "$(dirname "$0")"
export COMPOSE_PROFILES=kafka,bonus   # اگر Kafka/MinIO را نمی‌خواهید این خط را پاک کنید
CH() { docker compose exec -T clickhouse clickhouse-client -u student --password student --multiquery "$@"; }
run_sql() { echo "=== $1 ==="; CH < "$1"; }

docker compose up -d
until docker compose exec -T clickhouse clickhouse-client -u student --password student -q "SELECT 1" >/dev/null 2>&1; do sleep 2; done
./scripts/kafka.sh topic

run_sql sql/01_schema.sql
run_sql sql/02_load.sql
docker compose exec -T clickhouse ls -lh /var/lib/clickhouse/user_files      # سؤال ۳
bash scripts/small_inserts.sh                                                 # سؤال ۴
run_sql sql/03_changes.sql
run_sql sql/04_views_and_dictionary.sql
run_sql sql/05_queries.sql
run_sql sql/06_performance.sql
run_sql sql/07_kafka.sql
./scripts/kafka.sh send
run_sql sql/08_kafka_checks.sql
./scripts/kafka.sh describe                                                   # سؤال ۱۲
run_sql sql/09_bonus.sql