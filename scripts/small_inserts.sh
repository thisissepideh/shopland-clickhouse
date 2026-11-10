#!/usr/bin/env bash
# بخش ۲-ج: ۵۰۰ INSERT معمولی و ۵۰۰ INSERT با async_insert، و شمردن Partها از system.part_log
set -euo pipefail
URL='http://localhost:8123/?user=student&password=student'
q() { docker compose exec -T clickhouse clickhouse-client -u student --password student -q "$1"; }

count_parts() {  # $1 = زمان شروع
  q "SYSTEM FLUSH LOGS"
  q "SELECT count() FROM system.part_log WHERE event_type='NewPart' AND database='shop' AND table='small_inserts' AND event_time >= '$1'"
}

T0=$(q "SELECT now()")
for i in $(seq 1 500); do
  curl -s "$URL" --data-binary "INSERT INTO shop.small_inserts VALUES ($i, now())"
done
echo "[sync]  NewPart در part_log : $(count_parts "$T0")"
echo "[sync]  Partهای فعال همین الان: $(q "SELECT count() FROM system.parts WHERE database='shop' AND table='small_inserts' AND active")"

T1=$(q "SELECT now()")
for i in $(seq 501 1000); do
  curl -s "$URL&async_insert=1&wait_for_async_insert=0" --data-binary "INSERT INTO shop.small_inserts VALUES ($i, now())"
done
sleep 5   # صبر می‌کنیم بافر async خالی شود
echo "[async] NewPart در part_log : $(count_parts "$T1")"
echo "[async] کل ردیف‌ها (باید ۱۰۰۰ باشد): $(q "SELECT count() FROM shop.small_inserts")"
echo "[all]   Partهای فعال همین الان: $(q "SELECT count() FROM system.parts WHERE database='shop' AND table='small_inserts' AND active")"
