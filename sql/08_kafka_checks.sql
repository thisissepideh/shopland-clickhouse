-- سؤال ۱۲ و ۱۳ (بعد از فرستادن پیام‌ها با scripts/kafka.sh send)
SELECT sleep(3) FORMAT Null;

SELECT count() AS new_orders FROM shop.orders WHERE order_id >= 20000000;
SELECT sum(orders) AS delivered_orders_in_daily_sales_for_new_rows
FROM shop.daily_sales WHERE day = toDate(now());

-- Offset هر پارتیشن از دید ClickHouse
SELECT database, `table`, consumer_id,
       `assignments.topic` AS topics,
       `assignments.partition_id` AS partitions,
       `assignments.current_offset` AS current_offsets
FROM system.kafka_consumers;

-- پیام خراب
SELECT * FROM shop.orders_queue_errors;
