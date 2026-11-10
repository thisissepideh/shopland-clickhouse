#!/usr/bin/env bash
# مراحل Kafka: ساخت topic (قبل از 07_kafka.sql) یا فرستادن پیام‌ها (بعد از 07_kafka.sql)
set -euo pipefail
case "${1:-}" in
  topic)
    until docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list >/dev/null 2>&1; do sleep 2; done
    docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
      --bootstrap-server kafka:9092 --create --if-not-exists --topic orders --partitions 3
    ;;
  send)
    docker compose exec -T clickhouse clickhouse-client -u student --password student -q "
      SELECT 20000000 + number AS order_id, rand(1) % 200000 + 1 AS customer_id,
             rand(2) % 5000 + 1 AS product_id, 'Tehran' AS city, 'delivered' AS status,
             rand(5) % 5 + 1 AS quantity, (rand(6) % 2000 + 10) * 1000 AS unit_price,
             now() AS created_at
      FROM numbers(1000) FORMAT JSONEachRow" \
    | docker compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh \
        --bootstrap-server kafka:9092 --topic orders
    # یک پیام خراب
    echo 'this is not json' | docker compose exec -T kafka \
      /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka:9092 --topic orders
    ;;
  describe)
    docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh \
      --bootstrap-server kafka:9092 --describe --group orders_clickhouse
    ;;
  *) echo "usage: $0 topic|send|describe"; exit 1;;
esac
