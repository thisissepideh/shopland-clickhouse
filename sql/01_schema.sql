-- ============================================================
-- بخش ۱: طراحی جدول سفارش‌ها
-- ============================================================
CREATE DATABASE IF NOT EXISTS shop;

CREATE TABLE shop.orders
(
    order_id    UInt32      CODEC(T64, ZSTD(1)),   -- تا ۱۰ میلیون (حتی ۲۰ میلیون با Kafka) در UInt32 جا می‌شود
    customer_id UInt32      CODEC(T64, ZSTD(1)),   -- ~200 هزار مشتری
    product_id  UInt16      CODEC(T64, ZSTD(1)),   -- 5000 محصول < 65535
    city        LowCardinality(String),            -- فقط ۸ شهر تکراری
    status      Enum8('delivered' = 1, 'cancelled' = 2, 'returned' = 3),
    quantity    UInt8,                             -- ۱ تا ۵
    unit_price  UInt32      CODEC(T64, ZSTD(1)),   -- عدد صحیح تومان، حداکثر ~2 میلیون (پول را Float نمی‌کنیم)
    created_at  DateTime    CODEC(DoubleDelta, ZSTD(1))  -- دقت ثانیه، سال ۲۰۲۵
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(created_at)
ORDER BY (city, created_at);
-- نکته: status عمداً در ORDER BY نیست، چون ستون‌های کلید را نمی‌شود با ALTER UPDATE تغییر داد
-- (بخش ۳-ب: UPDATE status = 'cancelled').

-- جدول «ساده‌لوحانه» برای مقایسه‌ی حجم: همه String، بدون Codec، بدون ترتیب
CREATE TABLE shop.orders_naive
(
    order_id    String,
    customer_id String,
    product_id  String,
    city        String,
    status      String,
    quantity    String,
    unit_price  String,
    created_at  String
)
ENGINE = MergeTree
ORDER BY tuple();
