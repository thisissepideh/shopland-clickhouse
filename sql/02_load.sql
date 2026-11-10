-- ============================================================
-- بخش ۲: بارگذاری داده و فرمت‌ها
-- روش انتخابی: اول داده را بار می‌کنیم و بعد (در 04) Materialized Viewها را می‌سازیم
-- و داده‌ی قبلی را با INSERT ... SELECT در جدول مقصد می‌ریزیم (backfill).
-- ============================================================

-- الف) تولید ۱۰ میلیون سفارش (کوئری صورت پروژه بدون تغییر)
INSERT INTO shop.orders
SELECT
    number + 1                                              AS order_id,
    rand(1) % 200000 + 1                                    AS customer_id,
    rand(2) % 5000 + 1                                      AS product_id,
    ['Tehran','Tehran','Tehran','Mashhad',
     'Isfahan','Shiraz','Tabriz','Karaj'][rand(3) % 8 + 1]  AS city,
    ['delivered','delivered','delivered','delivered',
     'cancelled','returned'][rand(4) % 6 + 1]               AS status,
    rand(5) % 5 + 1                                         AS quantity,
    (rand(6) % 2000 + 10) * 1000                            AS unit_price,
    toDateTime('2025-01-01 00:00:00') + rand(7) % (365 * 86400) AS created_at
FROM numbers(10000000);

-- همان داده در جدول naive (همه چیز به String تبدیل می‌شود)
INSERT INTO shop.orders_naive
SELECT toString(order_id), toString(customer_id), toString(product_id), city,
       toString(status), toString(quantity), toString(unit_price), toString(created_at)
FROM shop.orders;

SELECT 'rows orders' AS what, count() FROM shop.orders
UNION ALL
SELECT 'rows orders_naive', count() FROM shop.orders_naive;

-- سؤال ۲: حجم فشرده / غیرفشرده‌ی هر ستون در دو جدول
SELECT table, name, type,
       formatReadableSize(data_compressed_bytes)   AS compressed,
       formatReadableSize(data_uncompressed_bytes) AS uncompressed,
       round(data_uncompressed_bytes / data_compressed_bytes, 2) AS ratio
FROM system.columns
WHERE database = 'shop' AND table IN ('orders', 'orders_naive')
ORDER BY table, name;

-- نسبت فشرده‌سازی کل هر جدول
SELECT table,
       formatReadableSize(sum(data_compressed_bytes))   AS compressed,
       formatReadableSize(sum(data_uncompressed_bytes)) AS uncompressed,
       round(sum(data_uncompressed_bytes) / sum(data_compressed_bytes), 2) AS ratio
FROM system.parts
WHERE database = 'shop' AND active AND table IN ('orders', 'orders_naive')
GROUP BY table;

-- ب) خروجی گرفتن: فایل‌ها در /var/lib/clickhouse/user_files ساخته می‌شوند
INSERT INTO FUNCTION file('orders_202503.parquet', 'Parquet')
SELECT * FROM shop.orders WHERE toYYYYMM(created_at) = 202503
SETTINGS engine_file_truncate_on_insert = 1;

INSERT INTO FUNCTION file('orders_202503.csv', 'CSVWithNames')
SELECT * FROM shop.orders WHERE toYYYYMM(created_at) = 202503
SETTINGS engine_file_truncate_on_insert = 1;

-- خواندن دوباره و مقایسه‌ی تعداد ردیف‌ها
SELECT (SELECT count() FROM file('orders_202503.parquet', 'Parquet')) AS parquet_rows,
       (SELECT count() FROM shop.orders WHERE toYYYYMM(created_at) = 202503) AS table_rows;

DESCRIBE file('orders_202503.parquet', 'Parquet');
DESCRIBE TABLE shop.orders;

-- ج) جدول مشکل INSERTهای کوچک (حلقه‌ها در scripts/small_inserts.sh اجرا می‌شوند)
CREATE TABLE shop.small_inserts (id UInt32, ts DateTime)
ENGINE = MergeTree ORDER BY id;
