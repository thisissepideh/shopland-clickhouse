-- ============================================================
-- بخش ۳: تغییر و حذف داده
-- ============================================================

-- الف) ReplacingMergeTree
CREATE TABLE shop.order_status_events
(
    order_id   UInt32,
    status     LowCardinality(String),
    updated_at DateTime
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY order_id;

-- سه INSERT جداگانه (سه Part جدا)
INSERT INTO shop.order_status_events
SELECT order_id, 'paid', created_at FROM shop.orders WHERE order_id % 20 = 0;

INSERT INTO shop.order_status_events
SELECT order_id, 'shipped', created_at + 3600 FROM shop.orders WHERE order_id % 40 = 0;

INSERT INTO shop.order_status_events
SELECT order_id, 'delivered', created_at + 86400 FROM shop.orders WHERE order_id % 80 = 0;

-- سؤال ۶ (هر کوئری با log_comment تا زمانش در query_log پیدا شود)
SELECT count() FROM shop.order_status_events SETTINGS log_comment = 'q6_count_nofinal';
SELECT count() FROM shop.order_status_events FINAL SETTINGS log_comment = 'q6_count_final';

SELECT status, count() AS orders
FROM shop.order_status_events FINAL
GROUP BY status ORDER BY status
SETTINGS log_comment = 'q6_status_final';

SELECT status, count() AS orders
FROM (
    SELECT order_id, argMax(status, updated_at) AS status
    FROM shop.order_status_events
    GROUP BY order_id
)
GROUP BY status ORDER BY status
SETTINGS log_comment = 'q6_status_argmax';

SYSTEM FLUSH LOGS;
SELECT log_comment, query_duration_ms, read_rows, formatReadableSize(memory_usage) AS mem
FROM system.query_log
WHERE type = 'QueryFinish' AND log_comment LIKE 'q6_%' AND event_date = today()
ORDER BY event_time;

-- ب) Mutation: مشتری 777 همه‌ی سفارش‌های ماه ۳ را لغو می‌کند
SELECT count() AS before_update FROM shop.orders
WHERE customer_id = 777 AND toYYYYMM(created_at) = 202503 AND status = 'cancelled';

ALTER TABLE shop.orders
    UPDATE status = 'cancelled'
    IN PARTITION 202503
    WHERE customer_id = 777;

-- پیگیری Mutation (is_done = 1 یعنی تمام شد؛ parts_to_do تعداد Partهای باقی‌مانده)
SELECT mutation_id, command, create_time, parts_to_do, is_done, latest_fail_reason
FROM system.mutations
WHERE database = 'shop' AND table = 'orders';

-- صبر تا پایان (فقط برای اینکه خروجی بعدی قطعی باشد)
SELECT sleep(3) FORMAT Null;

-- Lightweight Delete: مشتری 12345 حذف کامل
SELECT count() AS before_delete FROM shop.orders WHERE customer_id = 12345;
DELETE FROM shop.orders WHERE customer_id = 12345;
SELECT count() AS after_delete FROM shop.orders WHERE customer_id = 12345;

-- سؤال ۷: Mutation چند Part را بازنویسی کرد؟
SYSTEM FLUSH LOGS;
SELECT mutation_id, command, parts_to_do, is_done FROM system.mutations
WHERE database = 'shop' AND table = 'orders';

SELECT table, event_type, count() AS parts, sum(rows) AS rows, formatReadableSize(sum(size_in_bytes)) AS size
FROM system.part_log
WHERE database = 'shop' AND table = 'orders' AND event_type = 'MutatePart'
GROUP BY table, event_type;

-- حجم روی دیسک بعد از DELETE (بلافاصله کم نمی‌شود: فقط ماسک «حذف‌شده» ذخیره می‌شود)
SELECT formatReadableSize(sum(bytes_on_disk)) AS orders_size_on_disk
FROM system.parts WHERE database = 'shop' AND table = 'orders' AND active;

-- اختیاری و بدون نمره: Lightweight Update (نیاز به ستون‌های شماره‌ی بلاک/آفست)
-- CREATE TABLE shop.lw_update_demo (id UInt32, v String)
-- ENGINE = MergeTree ORDER BY id
-- SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;
-- INSERT INTO shop.lw_update_demo SELECT number, 'old' FROM numbers(10);
-- UPDATE shop.lw_update_demo SET v = 'new' WHERE id = 3;
-- SELECT * FROM shop.lw_update_demo ORDER BY id;

-- ج) TTL
CREATE TABLE shop.ttl_demo (id UInt32, ts DateTime)
ENGINE = MergeTree ORDER BY id
TTL ts + INTERVAL 1 MINUTE;

INSERT INTO shop.ttl_demo SELECT number, now() - 120 FROM numbers(10);   -- منقضی‌شده
INSERT INTO shop.ttl_demo SELECT 100 + number, now() FROM numbers(5);    -- زنده

SELECT count() AS before_optimize FROM shop.ttl_demo;
OPTIMIZE TABLE shop.ttl_demo FINAL;
SELECT count() AS after_optimize FROM shop.ttl_demo;

-- نکته: در نسخه‌های جدید ClickHouse ممکن است ردیف‌های منقضی‌شده همان لحظه‌ی INSERT حذف شوند
-- (تست محلی: before_optimize همان ۵ شد). برای دیدن رفتار «منقضی ولی هنوز دیده‌می‌شود»،
-- TTL را بعد از INSERT و بدون materialize اضافه می‌کنیم:
CREATE TABLE shop.ttl_demo2 (id UInt32, ts DateTime) ENGINE = MergeTree ORDER BY id;
INSERT INTO shop.ttl_demo2 SELECT number, now() - 120 FROM numbers(10);
INSERT INTO shop.ttl_demo2 SELECT 100 + number, now() FROM numbers(5);
ALTER TABLE shop.ttl_demo2 MODIFY TTL ts + INTERVAL 1 MINUTE SETTINGS materialize_ttl_after_modify = 0;

SELECT count() AS before_optimize_v2 FROM shop.ttl_demo2;   -- 15
OPTIMIZE TABLE shop.ttl_demo2 FINAL;
SELECT count() AS after_optimize_v2 FROM shop.ttl_demo2;    -- 5
