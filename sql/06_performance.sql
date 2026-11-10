-- ============================================================
-- بخش ۶: پیدا کردن کوئری کند و بهینه‌سازی
-- ============================================================

-- ۱) کوئری پشتیبانی را چند بار اجرا می‌کنیم (با برچسب perf_before)
SELECT order_id, product_id, status, quantity * unit_price AS amount, created_at
FROM shop.orders WHERE customer_id = 4321 ORDER BY created_at DESC
SETTINGS log_comment = 'perf_before' FORMAT Null;
SELECT order_id, product_id, status, quantity * unit_price AS amount, created_at
FROM shop.orders WHERE customer_id = 4321 ORDER BY created_at DESC
SETTINGS log_comment = 'perf_before' FORMAT Null;
SELECT order_id, product_id, status, quantity * unit_price AS amount, created_at
FROM shop.orders WHERE customer_id = 4321 ORDER BY created_at DESC
SETTINGS log_comment = 'perf_before' FORMAT Null;

SYSTEM FLUSH LOGS;
SELECT event_time, query_duration_ms, read_rows, formatReadableSize(read_bytes) AS read_bytes,
       formatReadableSize(memory_usage) AS memory
FROM system.query_log
WHERE type = 'QueryFinish' AND log_comment = 'perf_before' AND event_date = today()
ORDER BY event_time DESC LIMIT 5;

-- ۲) چند Granule خوانده می‌شود؟ (customer_id در کلید نیست => همه)
EXPLAIN indexes = 1
SELECT order_id, product_id, status, quantity * unit_price AS amount, created_at
FROM shop.orders WHERE customer_id = 4321 ORDER BY created_at DESC;

-- ۳) بهینه‌سازی بدون عوض کردن ORDER BY جدول orders:
--    یک جدول دوم با ORDER BY (customer_id, created_at) که با MV پر می‌شود
CREATE TABLE shop.orders_by_customer
(
    order_id    UInt32      CODEC(T64, ZSTD(1)),
    customer_id UInt32      CODEC(T64, ZSTD(1)),
    product_id  UInt16      CODEC(T64, ZSTD(1)),
    city        LowCardinality(String),
    status      Enum8('delivered' = 1, 'cancelled' = 2, 'returned' = 3),
    quantity    UInt8,
    unit_price  UInt32      CODEC(T64, ZSTD(1)),
    created_at  DateTime    CODEC(DoubleDelta, ZSTD(1))
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(created_at)
ORDER BY (customer_id, created_at);

CREATE MATERIALIZED VIEW shop.orders_by_customer_mv TO shop.orders_by_customer AS
SELECT order_id, customer_id, product_id, city, status, quantity, unit_price, created_at
FROM shop.orders;

-- backfill داده‌ی قبلی
INSERT INTO shop.orders_by_customer
SELECT order_id, customer_id, product_id, city, status, quantity, unit_price, created_at
FROM shop.orders;

-- دوباره اندازه‌گیری (perf_after)
SELECT order_id, product_id, status, quantity * unit_price AS amount, created_at
FROM shop.orders_by_customer WHERE customer_id = 4321 ORDER BY created_at DESC
SETTINGS log_comment = 'perf_after' FORMAT Null;
SELECT order_id, product_id, status, quantity * unit_price AS amount, created_at
FROM shop.orders_by_customer WHERE customer_id = 4321 ORDER BY created_at DESC
SETTINGS log_comment = 'perf_after' FORMAT Null;
SELECT order_id, product_id, status, quantity * unit_price AS amount, created_at
FROM shop.orders_by_customer WHERE customer_id = 4321 ORDER BY created_at DESC
SETTINGS log_comment = 'perf_after' FORMAT Null;

EXPLAIN indexes = 1
SELECT order_id, product_id, status, quantity * unit_price AS amount, created_at
FROM shop.orders_by_customer WHERE customer_id = 4321 ORDER BY created_at DESC;

SYSTEM FLUSH LOGS;
SELECT log_comment, count() AS runs, round(avg(query_duration_ms), 1) AS avg_ms,
       max(read_rows) AS read_rows, formatReadableSize(max(read_bytes)) AS read_bytes
FROM system.query_log
WHERE type = 'QueryFinish' AND log_comment IN ('perf_before', 'perf_after') AND event_date = today()
GROUP BY log_comment;

-- ۴) سه کوئری سنگین پروژه بر اساس read_bytes
SELECT normalized_query_hash,
       count() AS runs,
       formatReadableSize(max(read_bytes)) AS max_read_bytes,
       max(read_rows) AS read_rows,
       max(query_duration_ms) AS max_ms,
       substring(any(query), 1, 140) AS sample_query
FROM system.query_log
WHERE type = 'QueryFinish' AND query_kind = 'Select' AND event_date = today()
  AND query NOT LIKE '%system.%'
GROUP BY normalized_query_hash
ORDER BY max(read_bytes) DESC
LIMIT 3;
