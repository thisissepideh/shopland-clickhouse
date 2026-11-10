-- ============================================================
-- بخش اضافه: Kafka  (اول topic را بسازید: scripts/kafka.sh)
-- ============================================================
CREATE TABLE shop.orders_queue
(
    order_id    UInt32,
    customer_id UInt32,
    product_id  UInt16,
    city        String,
    status      String,
    quantity    UInt8,
    unit_price  UInt32,
    created_at  DateTime
)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka:9092',
         kafka_topic_list = 'orders',
         kafka_group_name = 'orders_clickhouse',
         kafka_format = 'JSONEachRow',
         kafka_num_consumers = 1,
         kafka_handle_error_mode = 'stream';

-- پیام‌های سالم -> orders
CREATE MATERIALIZED VIEW shop.orders_queue_mv TO shop.orders AS
SELECT order_id, customer_id, product_id, city, status, quantity, unit_price, created_at
FROM shop.orders_queue
WHERE length(_error) = 0;

-- پیام‌های خراب -> جدول خطا
CREATE TABLE shop.orders_queue_errors
(
    raw_message     String,
    kafka_topic     String,
    kafka_partition UInt64,
    kafka_offset    UInt64,
    error           String,
    received_at     DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (kafka_topic, kafka_partition, kafka_offset);

CREATE MATERIALIZED VIEW shop.orders_queue_errors_mv TO shop.orders_queue_errors AS
SELECT _raw_message AS raw_message,
       _topic       AS kafka_topic,
       _partition   AS kafka_partition,
       _offset      AS kafka_offset,
       _error       AS error
FROM shop.orders_queue
WHERE length(_error) > 0;
