#!/usr/bin/env bash
# اجرای کل پروژه از صفر:  ./run_all.sh
# اختیاری:  WITH_KAFKA=1 ./run_all.sh   و/یا   WITH_MINIO=1 ./run_all.sh
set -euo pipefail
cd "$(dirname "$0")"
P=""
[ "${WITH_KAFKA:-0}" = 1 ] && P="$P,kafka"
[ "${WITH_MINIO:-0}" = 1 ] && P="$P,bonus"
export COMPOSE_PROFILES="${P#,}"

CH() { docker compose exec -T clickhouse clickhouse-client -u student --password student --multiquery "$@"; }
run_sql() { echo "=== $1 ==="; CH < "$1"; }

docker compose up -d
until docker compose exec -T clickhouse clickhouse-client -u student --password student -q "SELECT 1" >/dev/null 2>&1; do sleep 2; done

run_sql sql/01_schema.sql
run_sql sql/02_load.sql
docker compose exec -T clickhouse ls -lh /var/lib/clickhouse/user_files      # سؤال ۳
bash scripts/small_inserts.sh                                                 # سؤال ۴
run_sql sql/03_changes.sql
run_sql sql/04_views_and_dictionary.sql
run_sql sql/05_queries.sql
run_sql sql/06_performance.sql
run_sql sql/09_bonus.sql

if [ "${WITH_KAFKA:-0}" = 1 ]; then
  ./scripts/kafka.sh topic
  run_sql sql/07_kafka.sql
  ./scripts/kafka.sh send
  run_sql sql/08_kafka_checks.sql
  ./scripts/kafka.sh describe                                                 # سؤال ۱۲
fi
if [ "${WITH_MINIO:-0}" = 1 ]; then
  run_sql sql/10_minio.sql
fi