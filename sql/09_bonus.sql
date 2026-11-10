-- ============================================================
-- کارهای امتیازی
-- ============================================================

-- ۱) CollapsingMergeTree: موجودی انبار
CREATE TABLE shop.stock
(
    product_id UInt16,
    qty        Int32,
    sign       Int8
)
ENGINE = CollapsingMergeTree(sign)
ORDER BY product_id;

INSERT INTO shop.stock SELECT 1, 100, 1;                    -- موجودی اولیه
INSERT INTO shop.stock SELECT * FROM values('product_id UInt16, qty Int32, sign Int8', (1, 100, -1), (1, 80, 1));       -- تغییر: ردیف قبلی ابطال، ردیف جدید ثبت

-- خواندن درست (مستقل از اینکه merge شده یا نه)
SELECT product_id, sum(qty * sign) AS current_qty
FROM shop.stock GROUP BY product_id HAVING sum(sign) > 0;
SELECT * FROM shop.stock FINAL;

-- ۲) موتور Merge
CREATE TABLE shop.orders_2024 AS shop.orders ENGINE = MergeTree
PARTITION BY toYYYYMM(created_at) ORDER BY (city, created_at);

INSERT INTO shop.orders_2024
SELECT 100000000 + number, rand(1) % 200000 + 1, rand(2) % 5000 + 1,
       ['Tehran','Tehran','Tehran','Mashhad','Isfahan','Shiraz','Tabriz','Karaj'][rand(3) % 8 + 1],
       ['delivered','delivered','delivered','delivered','cancelled','returned'][rand(4) % 6 + 1],
       rand(5) % 5 + 1, (rand(6) % 2000 + 10) * 1000,
       toDateTime('2024-01-01 00:00:00') + rand(7) % (366 * 86400)
FROM numbers(1000000);

CREATE TABLE shop.orders_all AS shop.orders
ENGINE = Merge('shop', '^orders(_2024)?$');

SELECT _table, toYear(created_at) AS year, count() AS orders
FROM shop.orders_all GROUP BY _table, year ORDER BY year;