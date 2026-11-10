-- ============================================================
-- بخش ۴: Materialized View و Dictionary
-- ترتیب: جدول مقصد -> MV -> backfill داده‌ی موجود با INSERT ... SELECT
-- ============================================================

-- الف) فروش روزانه با SummingMergeTree (فقط سفارش‌های delivered)
CREATE TABLE shop.daily_sales
(
    day        Date,
    city       LowCardinality(String),
    product_id UInt16,
    orders     UInt64,
    items      UInt64,
    revenue    UInt64
)
ENGINE = SummingMergeTree
PARTITION BY toYYYYMM(day)
ORDER BY (day, city, product_id);

CREATE MATERIALIZED VIEW shop.daily_sales_mv TO shop.daily_sales AS
SELECT toDate(created_at) AS day, city, product_id,
       count() AS orders,
       sum(quantity) AS items,
       sum(toUInt64(quantity) * unit_price) AS revenue
FROM shop.orders
WHERE status = 'delivered'
GROUP BY day, city, product_id;

INSERT INTO shop.daily_sales
SELECT toDate(created_at), city, product_id, count(), sum(quantity), sum(toUInt64(quantity) * unit_price)
FROM shop.orders
WHERE status = 'delivered'
GROUP BY toDate(created_at), city, product_id;

-- ب) مشتری‌های یکتای روزانه با AggregatingMergeTree
CREATE TABLE shop.daily_customers
(
    day        Date,
    customers  AggregateFunction(uniq, UInt32),
    avg_amount AggregateFunction(avg, UInt64)
)
ENGINE = AggregatingMergeTree
ORDER BY day;

CREATE MATERIALIZED VIEW shop.daily_customers_mv TO shop.daily_customers AS
SELECT toDate(created_at) AS day,
       uniqState(customer_id) AS customers,
       avgState(toUInt64(quantity) * unit_price) AS avg_amount
FROM shop.orders
GROUP BY day;

INSERT INTO shop.daily_customers
SELECT toDate(created_at), uniqState(customer_id), avgState(toUInt64(quantity) * unit_price)
FROM shop.orders
GROUP BY toDate(created_at);

-- جدول کمکی برای «مشتری جدید» (بخش ۵ سؤال ۷): اولین زمان سفارش هر مشتری
CREATE TABLE shop.customer_first_order
(
    customer_id UInt32,
    first_order AggregateFunction(min, DateTime)
)
ENGINE = AggregatingMergeTree
ORDER BY customer_id;

CREATE MATERIALIZED VIEW shop.customer_first_order_mv TO shop.customer_first_order AS
SELECT customer_id, minState(created_at) AS first_order
FROM shop.orders
GROUP BY customer_id;

INSERT INTO shop.customer_first_order
SELECT customer_id, minState(created_at) FROM shop.orders GROUP BY customer_id;

-- ج) Refreshable Materialized View: ۱۰ محصول پرفروش ۳۰ روز آخر «داده»
-- (داده تا ۳۱ دسامبر ۲۰۲۵ است، پس «آخرین روز داده» را از خود جدول می‌گیریم نه از today())
CREATE TABLE shop.top_products
(
    product_id UInt16,
    revenue    UInt64
)
ENGINE = MergeTree
ORDER BY product_id;

CREATE MATERIALIZED VIEW shop.top_products_mv
REFRESH EVERY 10 MINUTE
TO shop.top_products
AS
SELECT product_id, sum(revenue) AS revenue
FROM shop.daily_sales
WHERE day > (SELECT max(day) FROM shop.daily_sales) - 30
GROUP BY product_id
ORDER BY revenue DESC
LIMIT 10;

-- یک بار دستی refresh کنیم تا منتظر ۱۰ دقیقه نمانیم
SYSTEM REFRESH VIEW shop.top_products_mv;
SYSTEM WAIT VIEW shop.top_products_mv;

SELECT * FROM shop.top_products ORDER BY revenue DESC;
SELECT database, view, status, last_success_time, next_refresh_time, exception, read_rows, written_rows
FROM system.view_refreshes;

-- د) PostgreSQL
-- ۱) جدول با موتور PostgreSQL (هر کوئری مستقیم به PostgreSQL می‌رود)
CREATE TABLE shop.products_pg
(
    product_id Int32,
    title      String,
    category   String,
    brand      String
)
ENGINE = PostgreSQL('postgres:5432', 'shop', 'products', 'shop', 'shop');

SELECT * FROM shop.products_pg ORDER BY product_id LIMIT 3;

-- ۲) Dictionary: ۵۰۰۰ کلید کوچک است، پس FLAT مناسب‌ترین (و سریع‌ترین) layout است
CREATE DICTIONARY shop.products_dict
(
    product_id UInt64,
    title      String,
    category   String,
    brand      String
)
PRIMARY KEY product_id
SOURCE(POSTGRESQL(host 'postgres' port 5432 user 'shop' password 'shop' db 'shop' table 'products'))
LAYOUT(FLAT())
LIFETIME(MIN 300 MAX 600);   -- هر ۵ تا ۱۰ دقیقه از PostgreSQL دوباره خوانده می‌شود

SELECT dictGet('shop.products_dict', 'category', toUInt64(42)) AS category_of_42;
