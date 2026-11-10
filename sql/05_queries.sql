-- ============================================================
-- بخش ۵: کوئری‌های تحلیلی (تا جای ممکن از جدول‌های بخش ۴)
-- ============================================================

-- 1) درآمد ماهانه‌ی هر دسته‌ی محصول در ۲۰۲۵ (dictGet)
SELECT toStartOfMonth(day) AS month,
       dictGet('shop.products_dict', 'category', toUInt64(product_id)) AS category,
       sum(revenue) AS revenue
FROM shop.daily_sales
WHERE day >= '2025-01-01' AND day < '2026-01-01'
GROUP BY month, category
ORDER BY month, category;

-- 2) سه محصول پردرآمد هر دسته (LIMIT 3 BY)
SELECT category, product_id, revenue
FROM (
    SELECT dictGet('shop.products_dict', 'category', toUInt64(product_id)) AS category,
           product_id,
           sum(revenue) AS revenue
    FROM shop.daily_sales
    GROUP BY category, product_id
)
ORDER BY category, revenue DESC
LIMIT 3 BY category;

-- 3) به‌ازای هر شهر: درآمد، تعداد cancelled، درصد returned (Combinator)
-- این‌جا ناچاریم از orders بخوانیم، چون daily_sales فقط delivered دارد.
SELECT city,
       sumIf(toUInt64(quantity) * unit_price, status = 'delivered') AS revenue,
       countIf(status = 'cancelled') AS cancelled_orders,
       round(100 * countIf(status = 'returned') / count(), 2) AS returned_pct
FROM shop.orders
GROUP BY city
ORDER BY revenue DESC;

-- 4) ۱۰ مشتری با بیشترین خرید (delivered) + شناسه‌ی گران‌ترین سفارش (argMax)
SELECT customer_id,
       sum(toUInt64(quantity) * unit_price)           AS total_spent,
       argMax(order_id, toUInt64(quantity) * unit_price) AS priciest_order_id,
       max(toUInt64(quantity) * unit_price)           AS priciest_amount
FROM shop.orders
WHERE status = 'delivered'
GROUP BY customer_id
ORDER BY total_spent DESC
LIMIT 10;

-- 5) درآمد روزانه + تجمعی از اول سال + میانگین متحرک ۷ روزه (Window Function)
-- در ۶ روز اول، میانگین فقط روی روزهای موجود حساب می‌شود.
WITH daily AS (
    SELECT day, sum(revenue) AS revenue
    FROM shop.daily_sales
    GROUP BY day
)
SELECT day, revenue,
       sum(revenue) OVER (ORDER BY day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_revenue,
       round(avg(revenue) OVER (ORDER BY day ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 0) AS moving_avg_7d
FROM daily
ORDER BY day;

-- 6) رتبه‌ی شهرها در هر ماه + درصد تغییر نسبت به ماه قبل (rank و lagInFrame)
-- ماه اول ماه قبل ندارد: prev = 0 می‌شود و nullIf آن را NULL می‌کند، پس درصد تغییر NULL است.
SELECT month, city, revenue, rnk, prev_revenue,
       round(100 * (toInt64(revenue) - toInt64(prev_revenue)) / nullIf(prev_revenue, 0), 2) AS change_pct
FROM (
    SELECT month, city, revenue,
           rank() OVER (PARTITION BY month ORDER BY revenue DESC) AS rnk,
           lagInFrame(revenue, 1, 0) OVER (PARTITION BY city ORDER BY month
                                           ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS prev_revenue
    FROM (
        SELECT toStartOfMonth(day) AS month, city, sum(revenue) AS revenue
        FROM shop.daily_sales
        GROUP BY month, city
    )
)
ORDER BY month, rnk;

-- 7) مشتری‌های جدید هر ماه (اولین سفارش در همان ماه)
SELECT toStartOfMonth(first_order_time) AS month, count() AS new_customers
FROM (
    SELECT customer_id, minMerge(first_order) AS first_order_time
    FROM shop.customer_first_order
    GROUP BY customer_id
)
GROUP BY month
ORDER BY month;

-- ---------- کوئری‌های سؤال‌های ۹، ۱۰، ۱۱ ----------
-- سؤال ۹: بدون sum()/GROUP BY (ممکن است ردیف‌های ادغام‌نشده‌ی تکراری ببینید)
SELECT count() AS raw_rows, (SELECT count() FROM (SELECT 1 FROM shop.daily_sales GROUP BY day, city, product_id)) AS distinct_keys
FROM shop.daily_sales;
SELECT sum(revenue) AS naive_sum_no_group FROM shop.daily_sales WHERE day = '2025-03-10' AND city = 'Tehran' AND product_id = 1;

-- سؤال ۱۰: مشتری یکتا در هر ماه: uniqMerge در برابر uniqExact
SELECT toStartOfMonth(day) AS month, uniqMerge(customers) AS approx_unique
FROM shop.daily_customers GROUP BY month ORDER BY month
SETTINGS log_comment = 'q10_uniqMerge';

SELECT toStartOfMonth(created_at) AS month, uniqExact(customer_id) AS exact_unique
FROM shop.orders GROUP BY month ORDER BY month
SETTINGS log_comment = 'q10_uniqExact';

-- (نمونه‌ی اشتباه: جمع عدد یکتای روزها بیش از حد می‌شمارد، چون یک مشتری در چند روز تکرار می‌شود)
SELECT toStartOfMonth(day) AS month, sum(daily_unique) AS wrong_sum_of_daily_uniques
FROM (SELECT day, uniqMerge(customers) AS daily_unique FROM shop.daily_customers GROUP BY day)
GROUP BY month
ORDER BY month;

-- سؤال ۱۱: JOIN با جدول PostgreSQL در برابر dictGet
SELECT p.category, sum(toUInt64(o.quantity) * o.unit_price) AS revenue
FROM shop.orders AS o
INNER JOIN shop.products_pg AS p ON o.product_id = p.product_id
WHERE o.status = 'delivered'
GROUP BY p.category ORDER BY p.category
SETTINGS log_comment = 'q11_join_pg';

SELECT dictGet('shop.products_dict', 'category', toUInt64(product_id)) AS category,
       sum(toUInt64(quantity) * unit_price) AS revenue
FROM shop.orders
WHERE status = 'delivered'
GROUP BY category ORDER BY category
SETTINGS log_comment = 'q11_dictGet';

SYSTEM FLUSH LOGS;
SELECT log_comment, query_duration_ms, read_rows, formatReadableSize(memory_usage) AS mem
FROM system.query_log
WHERE type = 'QueryFinish' AND log_comment IN ('q10_uniqMerge','q10_uniqExact','q11_join_pg','q11_dictGet')
  AND event_date = today()
ORDER BY event_time;
