# گزارش پروژه‌ی پایانی ClickHouse — انبار داده‌ی «شاپ‌لند»

> کوئری‌ها در پوشه‌ی `sql/` هستند و اینجا هم آمده‌اند. خروجی‌ها از اجرای واقعی روی ClickHouse `26.8.36.3` گرفته شده‌اند.
> چون داده تصادفی است، عددهای دیگران فرق می‌کند؛ روش و توضیح مهم است.
> موارد علامت‌خورده با **«TODO»** هنوز اجرا نشده‌اند (فقط MinIO که اختیاری است).

## ۰. محیط و نسخه‌ها

| سرویس | نسخه |
|---|---|
| ClickHouse | `26.8.36.3` (ایمیج `clickhouse/clickhouse-server:26.8`) |
| PostgreSQL | `17` |
| Kafka / MinIO | `apache/kafka:4.0.0` / `minio/minio` (بخش‌های اختیاری، پشت profile در compose) |

- کاربر ClickHouse: `student` / `student`. دو سرویس روی شبکه‌ی پیش‌فرض compose‌اند و ClickHouse با نام `postgres` به PostgreSQL وصل می‌شود.
- اجرا از صفر: `docker compose up -d` و بعد فایل‌های `sql/01..09` به ترتیب (یا `./run_all.sh`). Kafka و MinIO فقط با `WITH_KAFKA=1` و `WITH_MINIO=1` (یا `COMPOSE_PROFILES=kafka,bonus`) بالا می‌آیند.
- **بررسی بخش ۰:** `docker compose ps` نشان داد `shopland-clickhouse` در حال اجرا و `shopland-postgres` در وضعیت `healthy` است؛ `SELECT version()` برابر `26.8.36.3` و `SELECT count(*) FROM products` برابر `5000` شد.

**روش بخش ۲ (قبل یا بعد از MV):** اول ۱۰ میلیون سفارش را در `orders` بار کردم (فایل 02) و بعد Materialized Viewها را در فایل 04 ساختم و داده‌ی قبلی را با `INSERT ... SELECT` به جدول مقصد ریختم (backfill). دلیل: ترتیب فایل‌ها ثابت است و MV فقط داده‌ی بعد از ساخته شدنش را می‌بیند. همین روش برای `orders_by_customer` و `customer_spend` در بخش ۶ هم استفاده شد.

---

## بخش ۱: طراحی جدول (`sql/01_schema.sql`)

```
CREATE TABLE shop.orders
(
    order_id    UInt32   CODEC(T64, ZSTD(1)),
    customer_id UInt32   CODEC(T64, ZSTD(1)),
    product_id  UInt16   CODEC(T64, ZSTD(1)),
    city        LowCardinality(String),
    status      Enum8('delivered' = 1, 'cancelled' = 2, 'returned' = 3),
    quantity    UInt8,
    unit_price  UInt32   CODEC(T64, ZSTD(1)),
    created_at  DateTime CODEC(DoubleDelta, ZSTD(1))
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(created_at)
ORDER BY (city, created_at)
```

| ستون | نوع | دلیل |
|---|---|---|
| order_id | `UInt32` | مثبت و تا ۲۰ میلیون (با Kafka) در ۳۲ بیت جا می‌شود |
| customer_id | `UInt32` | حدود ۲۰۰ هزار مقدار |
| product_id | `UInt16` | ۵۰۰۰ محصول |
| city | `LowCardinality(String)` | فقط ۸ مقدار تکراری |
| status | `Enum8` | سه مقدار ثابت |
| quantity | `UInt8` | ۱ تا ۵ |
| unit_price | `UInt32` | عدد صحیح تومان، حداکثر حدود ۲ میلیون (برای پول از Float استفاده نمی‌کنیم) |
| created_at | `DateTime` | دقت ثانیه |

- **Codec:** `DoubleDelta+ZSTD` برای `created_at`، چون داخل هر شهر مرتب است و اختلاف‌های کوچکی دارد؛ `T64+ZSTD` برای ستون‌های عددی `order_id`، `customer_id`، `product_id` و `unit_price`.
- **PARTITION BY ماهانه:** کوئری‌های ماهانه فقط پارتیشن لازم را می‌خوانند (۱۲ پارتیشن، کم و مناسب).
- **ORDER BY `(city, created_at)`:** `status` عمداً در کلید نیست، چون ستون‌های کلید را نمی‌شود با `ALTER ... UPDATE` عوض کرد و بخش ۳-ب دقیقاً `status` را آپدیت می‌کند.
- `orders_naive`: همه‌ی ستون‌ها `String`، بدون Codec، `ORDER BY tuple()`.

### سؤال ۱ — انتخاب ORDER BY

**کوئری‌ها:**
```sql
EXPLAIN indexes = 1 SELECT count() FROM shop.orders
WHERE city = 'Tehran' AND created_at >= '2025-03-01' AND created_at < '2025-03-08';

EXPLAIN indexes = 1 SELECT count() FROM shop.orders WHERE customer_id = 4321;
```

**خروجی (خلاصه):**

| کوئری | Min-Max | Partition | PrimaryKey | Granule خوانده‌شده |
|---|---|---|---|---|
| `city` + بازه‌ی یک هفته | ۱۰۴ / ۱۲۲۳ | ۱۰۴ / ۱۰۴ | **۱۰ / ۱۰۴** (جست‌وجوی دودویی) | **۱۰ از ۱۲۲۳** |
| `customer_id = 4321` | ۱۲۲۳ / ۱۲۲۳ | ۱۲۲۳ / ۱۲۲۳ | ۱۲۲۳ / ۱۲۲۳ (`Condition: true`) | **۱۲۲۳ از ۱۲۲۳** |

**توضیح:** Sparse Index برای هر Granule (۸۱۹۲ ردیف) فقط یک مقدار از کلید نگه می‌دارد و با آن Granuleهای بی‌ربط رد می‌شوند. ستون کم‌تنوع `city` اول آمده تا داده‌ی هر شهر پشت هم باشد و داخل هر شهر به `created_at` مرتب است؛ کوئری اول با Min-Max و Partition اول فقط مارس را نگه می‌دارد و PrimaryKey آن را به ۱۰ Granule (حدود ۱٪ جدول) می‌رساند. کوئری‌هایی که روی `city` یا `city + بازه‌ی زمانی` فیلتر می‌کنند سود می‌برند، ولی کوئری روی `customer_id`، `product_id` یا `status` به‌تنهایی چون در کلید نیستند کل Granuleها را می‌خوانند (مثال دوم). همین کوئری پایه‌ی بخش ۶ است.

### سؤال ۲ — حجم ستون‌ها در دو جدول

**کوئری:** آخر `sql/02_load.sql` (`system.columns` و `system.parts`).

**خروجی:**

| ستون | `orders` فشرده | `orders` غیرفشرده | `orders_naive` فشرده | `orders_naive` غیرفشرده |
|---|---|---|---|---|
| city | 38.33 KiB | 7.55 MiB | 618.62 KiB | 134.71 MiB |
| created_at | 8.33 MiB | 30.12 MiB | 55.98 MiB | 257.49 MiB |
| customer_id | 16.98 MiB | 30.12 MiB | 65.07 MiB | 128.22 MiB |
| order_id | 22.07 MiB | 30.12 MiB | 65.13 MiB | 141.99 MiB |
| product_id | 12.25 MiB | 15.06 MiB | 43.79 MiB | 112.33 MiB |
| quantity | 5.33 MiB | 7.53 MiB | 7.10 MiB | 85.83 MiB |
| status | 4.48 MiB | 7.53 MiB | 24.00 MiB | 160.53 MiB |
| unit_price | 18.65 MiB | 30.12 MiB | 48.13 MiB | 137.90 MiB |
| **کل جدول** | **111.98 MiB** | **200.30 MiB** | **309.80 MiB** | **1.13 GiB** |

| جدول | نسبت فشرده‌سازی کل |
|---|---|
| `orders` | 1.79 |
| `orders_naive` | 3.74 |

**توضیح:** `orders` از نظر حجم فشرده‌ی نهایی حدود **۲.۸ برابر** کوچک‌تر است (۱۱۲ در برابر ۳۱۰ مگابایت) و حجم غیرفشرده‌اش حدود ۶ برابر کمتر است. نسبت فشرده‌سازی `naive` بالاتر است، ولی این گمراه‌کننده است: رشته‌ها پر از کاراکتر تکراری‌اند و راحت فشرده می‌شوند، اما از اول بزرگ‌تر بوده‌اند؛ برای مقایسه باید حجم نهایی را دید. بیشترین تفاوت از نظر حجم فشرده در `customer_id` (۴۸ مگابایت) و `created_at` (۴۷.۷ مگابایت) است؛ از نظر نسبت `city` (حدود ۱۶ برابر، به خاطر `LowCardinality`)، و از نظر حجم غیرفشرده `created_at` (۲۵۷ در برابر ۳۰ مگابایت). دلیل `created_at`: در `naive` رشته‌ی ۱۹ کاراکتری است ولی در `orders` عدد ۴ بایتی که داخل هر شهر مرتب است و `DoubleDelta` اختلاف‌های کوچک ثانیه‌ها را فشرده می‌کند. نسبت `orders` پایین است (۱.۷۹) چون داده تصادفی است؛ `order_id` هم ضعیف فشرده شد (۱.۳۶)، چون جدول به `(city, created_at)` مرتب است و `order_id` داخل هر شهر تصادفی دیده می‌شود، پس `Delta` کمکی نمی‌کند.

---

## بخش ۲: بارگذاری داده و فرمت‌ها (`sql/02_load.sql`, `scripts/small_inserts.sh`)

- ۱۰ میلیون سفارش با کوئری صورت پروژه (بدون تغییر) در `orders` بار شد؛ `orders_naive` هم ۱۰ میلیون ردیف دارد. کل اجرای فایل ۰۲ حدود ۲۹ ثانیه طول کشید.

### سؤال ۳ — Parquet و CSV

**کوئری:**
```sql
INSERT INTO FUNCTION file('orders_202503.parquet','Parquet') SELECT * FROM shop.orders WHERE toYYYYMM(created_at)=202503;
INSERT INTO FUNCTION file('orders_202503.csv','CSVWithNames') SELECT * FROM shop.orders WHERE toYYYYMM(created_at)=202503;
SELECT count() FROM file('orders_202503.parquet','Parquet');
DESCRIBE file('orders_202503.parquet','Parquet');
```

**خروجی:**
```
-rw-r----- 1 clickhouse clickhouse 59M orders_202503.csv
-rw-r----- 1 clickhouse clickhouse 14M orders_202503.parquet

parquet_rows = 850569   table_rows = 850569
```

| ستون | نوع در جدول اصلی | نوع در `DESCRIBE` فایل Parquet |
|---|---|---|
| order_id، customer_id، product_id، quantity، unit_price | `UInt32`/`UInt16`/`UInt8` | همان نوع‌ها |
| city | `LowCardinality(String)` | `String` |
| status | `Enum8(...)` | `String` |
| created_at | `DateTime` | `DateTime64(3, 'UTC')` |

**توضیح:** Parquet ستونی است و هر ستون جدا فشرده می‌شود، پس فایل Parquet (۱۴ مگابایت) حدود **۴.۲ برابر** کوچک‌تر از CSV متنی (۵۹ مگابایت) شد. تعداد ردیف‌ها برابر است. Parquet نوع‌های `Enum` و `LowCardinality` را نمی‌شناسد، پس `city` و `status` رشته شدند، و زمان با دقت میلی‌ثانیه و منطقه‌ی زمانی (`DateTime64(3, 'UTC')`) ذخیره شده است.

### سؤال ۴ — Part و INSERT کوچک

**کوئری:** `bash scripts/small_inserts.sh` (شمردن `NewPart` در `system.part_log` بعد از `SYSTEM FLUSH LOGS`).

**خروجی:**

| حالت | Part ساخته‌شده (`part_log`) | Part فعال همان لحظه |
|---|---|---|
| ۵۰۰ INSERT معمولی | **۵۰۰** | ۵ |
| ۵۰۰ INSERT با `async_insert=1` | **۵۰** | ۵ |

کل ردیف‌ها ۱۰۰۰ شد.

**توضیح:** هر INSERT معمولی یک Part جدا می‌سازد (۵۰۰)؛ با `async_insert` سرور INSERTها را در بافر جمع و دسته‌ای می‌نویسد (۵۰ Part). `system.parts` فقط Partهای **فعال** را نشان می‌دهد و ادغام‌های پس‌زمینه Partهای کوچک را در هم ریخته‌اند، ولی `part_log` تاریخچه‌ی ساخته‌شدن را دارد، برای همین عدد خیلی کمتر است. اگر این INSERTها هم‌زمان و از چند برنامه انجام شوند، ادغام‌ها عقب می‌مانند و ClickHouse اول INSERT را کند می‌کند و بعد خطای `Too many parts` می‌دهد.

### سؤال ۵ — `wait_for_async_insert=0`

**توضیح:** با مقدار ۰ سرور قبل از نوشتن داده روی دیسک به کلاینت «موفق» می‌گوید (fire-and-forget). اگر سرور قبل از Flush بافر خراب شود یا INSERT بعداً خطا بدهد (مثلاً خطای نوع داده)، داده بی‌صدا گم می‌شود و کلاینت خبردار نمی‌شود. در یک سیستم واقعی برای داده‌ای که از دست رفتنش قابل قبول نیست (سفارش، پرداخت) مقدار را `1` می‌گذارم و `0` را فقط برای لاگ و متریکی که از دست رفتن بخشی از آن مهم نیست نگه می‌دارم.

---

## بخش ۳: تغییر و حذف داده (`sql/03_changes.sql`)

### سؤال ۶ — ReplacingMergeTree و FINAL

**کوئری‌ها:** `count()` بدون `FINAL` و با `FINAL`؛ شمارش وضعیت‌ها با `FINAL` و با `argMax(status, updated_at) ... GROUP BY order_id`.

**خروجی:**

| کوئری | نتیجه | زمان | حافظه |
|---|---|---|---|
| `count()` بدون `FINAL` | **875,000** | 2 ms | 103 KiB |
| `count()` با `FINAL` | **500,000** | 71 ms | 9.31 MiB |
| وضعیت‌ها با `FINAL` | delivered 125,000، paid 250,000، shipped 125,000 | 82 ms | 10.59 MiB |
| وضعیت‌ها با `argMax` | همان نتیجه | 328 ms | 41.27 MiB |

**توضیح:** دو روش `FINAL` و `argMax` نتیجه‌ی یکسان می‌دهند. `ReplacingMergeTree` ردیف‌های تکراری را فقط هنگام ادغام‌های پس‌زمینه حذف می‌کند و زمانش قابل پیش‌بینی نیست، پس بدون `FINAL` هر سه نسخه‌ی سفارش‌ها (500k + 250k + 125k = 875k) شمرده می‌شود و عدد اشتباه است. زمان ۲ms شمارش بدون `FINAL` منصفانه نیست، چون فقط متادیتای Partها خوانده شد (`read_rows = 1`)؛ مقایسه‌ی درست بین `FINAL` و `argMax` است که در این اجرا `FINAL` سریع‌تر و کم‌مصرف‌تر بود.

### سؤال ۷ — Mutation و Lightweight Delete

**کوئری:**
```sql
ALTER TABLE shop.orders UPDATE status = 'cancelled' IN PARTITION 202503 WHERE customer_id = 777;
SELECT * FROM system.mutations WHERE table = 'orders';
DELETE FROM shop.orders WHERE customer_id = 12345;
SELECT count() FROM system.part_log WHERE event_type = 'MutatePart' AND table = 'orders';
```

**خروجی:**

- مشتری ۷۷۷ در ماه ۳: قبل از UPDATE یک سفارش `cancelled` بود و بعد از آن هر ۳ سفارش مارس `cancelled` شد.
- `mutation_109` (UPDATE): در اولین نمایش `parts_to_do = 33` و `is_done = 0`، و بعد از چند ثانیه `parts_to_do = 0` و `is_done = 1`.
- `mutation_110`: دستور `DELETE FROM` به‌صورت داخلی `UPDATE _row_exists = 0 WHERE customer_id = 12345` ثبت شد. مشتری ۱۲۳۴۵ قبل از حذف ۴۷ سفارش داشت و بعد از آن ۰.
- `system.part_log`: ۴۵ رویداد `MutatePart` (برای هر دو Mutation) با مجموع ۸٬۸۰۷٬۹۵۵ ردیف و ۱۴۹.۷۲ MiB.
- حجم `orders` روی دیسک بعد از `DELETE`: ۱۱۲.۶۲ MiB (کم نشد).

**توضیح:** Mutation Partهای حاوی ردیف مورد نظر را بازنویسی می‌کند و ناهم‌زمان انجام می‌شود، برای همین `parts_to_do` تا پایان کار صفر نیست. چون هر `ALTER ... UPDATE` یک کار سنگین روی Partهاست، سفارش‌ها را یکی‌یکی آپدیت نمی‌کنیم؛ باید یک Mutation دسته‌ای بزنیم یا مدل داده را append-only کنیم (مثل `ReplacingMergeTree`). `DELETE FROM` فقط ردیف‌ها را با ماسک «حذف‌شده» (`_row_exists`) علامت می‌زند، پس حجم روی دیسک فوراً کم نمی‌شود؛ فضا فقط بعد از ادغام و بازنویسی Partها آزاد می‌شود.

### سؤال ۸ — TTL

**کوئری:** `shop.ttl_demo` (TTL در `CREATE`) و `shop.ttl_demo2` (TTL بعد از INSERT با `materialize_ttl_after_modify = 0`)، هر دو با `OPTIMIZE TABLE ... FINAL`.

**خروجی:**

| جدول | قبل از OPTIMIZE | بعد از OPTIMIZE |
|---|---|---|
| `ttl_demo` | 5 | 5 |
| `ttl_demo2` | **15** | **5** |

**توضیح:** در `ClickHouse 26.8` وقتی TTL از اول روی جدول باشد، ردیف منقضی همان لحظه‌ی INSERT حذف می‌شود، پس `ttl_demo` از اول ۵ ردیف دارد. در `ttl_demo2` که TTL بعد از INSERT اضافه شد، ردیف‌های منقضی تا قبل از `OPTIMIZE` دیده می‌شوند (۱۵)، چون TTL هنگام **ادغام** Partها اعمال می‌شود، نه هنگام خواندن. اگر `TTL created_at + INTERVAL 1 YEAR DELETE` را به `orders` (که همه‌ی داده‌اش مربوط به ۲۰۲۵ است) اضافه کنیم، ردیف‌های قدیمی‌تر از یک سال نسبت به روز اجرا در ادغام‌های بعدی (و با `ALTER MODIFY TTL` به‌صورت Mutation) حذف می‌شوند؛ هر روز که می‌گذرد داده‌ی بیشتری منقضی می‌شود و در نهایت کل جدول خالی می‌شود.

---

## بخش ۴: MV و Dictionary (`sql/04_views_and_dictionary.sql`)

- `daily_sales` (`SummingMergeTree`) با `daily_sales_mv` فقط سفارش‌های `delivered`؛ **۴٬۷۰۳٬۸۷۴ ردیف**.
- `daily_customers` (`AggregatingMergeTree`) با `uniqState` و `avgState` و MV مربوطه.
- `customer_first_order` (جدول کمکی با `minState`) برای «مشتری جدید» در سؤال ۷ بخش ۵.
- `top_products_mv`: `REFRESH EVERY 10 MINUTE`. چون داده تا ۳۱ دسامبر ۲۰۲۵ است، «۳۰ روز آخر» را نسبت به `max(day)` جدول حساب می‌کنم، نه `today()`.
- `products_pg` (موتور PostgreSQL) و `products_dict` (`LAYOUT(FLAT)`، `LIFETIME(MIN 300 MAX 600)`).

**خروجی:**
```
top_products (۱۰ محصول پرفروش، سه ردیف اول):
 2835 | 524539000      1993 | 495635000      4677 | 475176000

system.view_refreshes:
 shop | top_products_mv | Scheduled | last_success 2026-11-09 20:24:12 | next 2026-11-09 20:30:00 | exception '' | read_rows 398776 | written_rows 10

products_pg (۳ ردیف اول):  1 product-1 Laptop brand-1 | 2 product-2 Book brand-2 | 3 product-3 Clothes brand-3
dictGet('shop.products_dict','category',42) = Mobile
```

### سؤال ۹ — چرا هنوز `sum()` و `GROUP BY`

**توضیح:** `SummingMergeTree` ردیف‌هایی با کلید یکسان را فقط هنگام ادغام جمع می‌کند، پس هر لحظه ممکن است چند ردیف جزئی برای یک کلید وجود داشته باشد. اگر `sum()` و `GROUP BY` ننویسید، به‌جای مقدار کل، فقط مقدار یکی از ردیف‌های جزئی را می‌بینید (عدد کمتر از واقعی).

**نکته درباره‌ی اجرای من:** در `daily_sales` بعد از backfill و ادغام‌ها تعداد ردیف‌ها (`4,703,874`) با تعداد کلیدهای یکتا (`4,703,874`) برابر بود، یعنی ادغام انجام شده و تکراری نمی‌دیدیم؛ پس مشکل را با یک جدول نمونه‌ی کوچک نشان می‌دهم.

**کوئری نمونه:** یک جدول کوچک `SummingMergeTree` با ادغام موقتاً متوقف‌شده (`SYSTEM STOP MERGES`) و دو INSERT جدا روی یک کلید:
```sql
CREATE TABLE shop.sales_demo (day Date, city String, product_id UInt16, orders UInt64, revenue UInt64)
ENGINE = SummingMergeTree ORDER BY (day, city, product_id);
SYSTEM STOP MERGES shop.sales_demo;
INSERT INTO shop.sales_demo SELECT toDate('2025-03-10'), 'Tehran', 1, 1, 1000;
INSERT INTO shop.sales_demo SELECT toDate('2025-03-10'), 'Tehran', 1, 2, 5000;

SELECT * FROM shop.sales_demo ORDER BY orders;                      -- بدون sum و GROUP BY
SELECT day, city, product_id, sum(orders) AS orders, sum(revenue) AS revenue
FROM shop.sales_demo GROUP BY day, city, product_id;                -- با sum و GROUP BY
```

**خروجی:**

بدون `sum()` و `GROUP BY` (دو ردیف جزئی برای یک کلید):

| day | city | product_id | orders | revenue |
|---|---|---|---|---|
| 2025-03-10 | Tehran | 1 | 1 | 1000 |
| 2025-03-10 | Tehran | 1 | 2 | 5000 |

با `sum()` و `GROUP BY`:

| day | city | product_id | orders | revenue |
|---|---|---|---|---|
| 2025-03-10 | Tehran | 1 | **3** | **6000** |

**نتیجه:** تا وقتی ادغام انجام نشده، هر INSERT ردیف خودش را دارد؛ اگر `sum()` و `GROUP BY` ننویسیم فقط مقدار جزئی یک ردیف (مثلاً `2` سفارش و `5000`) را می‌بینیم، نه مقدار کل (`3` و `6000`).

---

### سؤال ۱۰ — `uniqMerge` در برابر `uniqExact`

**خروجی:**

| | ژانویه | زمان | ردیف خوانده‌شده | حافظه |
|---|---|---|---|---|
| `uniqMerge` از `daily_customers` | 197,059 | 473 ms | 365 | 101 MiB |
| `uniqExact` از `orders` | 197,184 | 886 ms | 10,000,000 | 65.6 MiB |
| جمع عدد یکتای روزها (اشتباه) | 799,287 | | | |

در همه‌ی ماه‌ها `uniqMerge` کمتر از حدود ۰.۱۳٪ با `uniqExact` فرق دارد (مثلاً مارس 197,019 در برابر 197,242). جمع عدد یکتای روزها هر ماه ۷۲۰ تا ۷۹۹ هزار شد، یعنی حدود ۴ برابر مقدار واقعی.

**توضیح:** `uniqMerge` فقط ۳۶۵ ردیف خلاصه می‌خواند و حدود ۱.۹ برابر سریع‌تر بود، ولی تقریبی است (خطای کم)؛ حافظه‌اش در این اجرا بیشتر شد چون باید حالت‌های `uniq` ادغام شوند. `uniqExact` روی همه‌ی ۱۰ میلیون سفارش دقیق است ولی کندتر. عدد یکتای روزها را نمی‌شود جمع کرد چون یک مشتری در چند روز مختلف سفارش می‌دهد و چند بار شمرده می‌شود؛ `uniqState` خود مجموعه‌ی (تقریبی) مشتری‌ها را نگه می‌دارد و `uniqMerge` اشتراک آن‌ها را درست حساب می‌کند.

### سؤال ۱۱ — JOIN با PostgreSQL در برابر `dictGet`

**خروجی:**

| روش | زمان | ردیف خوانده‌شده | حافظه |
|---|---|---|---|
| JOIN با `products_pg` | 1,063 ms | 10,005,000 | 15.28 MiB |
| `dictGet` | 814 ms | 10,000,000 | 2.91 MiB |

نتیجه‌ی هر دو (درآمد هر دسته) یکسان بود.

**توضیح:** `dictGet` فقط یک جست‌وجوی hash در حافظه است، ولی JOIN با `products_pg` هر بار ۵۰۰۰ ردیف را از PostgreSQL می‌خواند؛ در این اجرا Dictionary حدود ۱.۳ برابر سریع‌تر و حدود ۵ برابر کم‌مصرف‌تر بود. اختلاف زمان کوچک است چون جدول محصولات کوچک است و روی همان ماشین؛ با جدول بزرگ‌تر یا شبکه‌ی کندتر بیشتر می‌شود. برای گزارش‌های پرتکرار Dictionary مناسب‌تر است. تغییر نام محصول در PostgreSQL بعد از refresh بعدی Dictionary (حداکثر `LIFETIME MAX` = ۶۰۰ ثانیه) دیده می‌شود، یا فوراً با `SYSTEM RELOAD DICTIONARY shop.products_dict`.

---

## بخش ۵: کوئری‌های تحلیلی (`sql/05_queries.sql`)

| # | موضوع | تکنیک | جدول |
|---|---|---|---|
| ۱ | درآمد ماهانه‌ی هر دسته | `dictGet` | `daily_sales` |
| ۲ | سه محصول پردرآمد هر دسته | `LIMIT 3 BY` | `daily_sales` |
| ۳ | درآمد، cancelled، درصد returned هر شهر | `sumIf`, `countIf` | `orders` |
| ۴ | ۱۰ مشتری برتر + گران‌ترین سفارش | `argMax` | `orders` |
| ۵ | درآمد روزانه، تجمعی، میانگین متحرک ۷ روزه | Window Function | `daily_sales` |
| ۶ | رتبه‌ی شهر و درصد تغییر ماهانه | `rank`, `lagInFrame` | `daily_sales` |
| ۷ | مشتری‌های جدید هر ماه | `minMerge` | `customer_first_order` |

کوئری ۳ و ۴ ناچار از `orders` می‌خوانند، چون `daily_sales` فقط سفارش‌های `delivered` را دارد و برای `cancelled` و `returned` داده ندارد.

**خروجی (نمونه):**

**۱.** هر ماه ۶ ردیف (یک ردیف برای هر دسته)؛ مثلاً ژانویه: Book `288,313,481,000`، Clothes `289,538,610,000`، Food `287,977,936,000`، Laptop `286,302,169,000`، Mobile `287,341,730,000`، Toys `287,571,117,000`. فوریه کمتر است (حدود ۲۵۸ تا ۲۶۱ میلیارد)، چون ۲۸ روز دارد.

**۲.** سه محصول اول هر دسته، مثلاً Book: `4454` (4,602,471,000)، `3932` (4,576,476,000)، `3020` (4,504,158,000).

**۳.**

| city | revenue | cancelled_orders | returned_pct |
|---|---|---|---|
| Tehran | 7,566,725,748,000 | 624,275 | 16.69 |
| Mashhad | 2,528,317,031,000 | 208,397 | 16.67 |
| Tabriz | 2,524,405,440,000 | 209,110 | 16.67 |
| Shiraz | 2,520,426,180,000 | 209,197 | 16.72 |
| Isfahan | 2,519,653,313,000 | 208,692 | 16.67 |
| Karaj | 2,518,579,481,000 | 208,680 | 16.70 |

(تهران حدود ۳ برابر شهرهای دیگر است، چون در آرایه‌ی تولید داده سه بار تکرار شده؛ درصد returned همه‌ی شهرها حدود یک‌ششم است.)

**۴.**

| customer_id | total_spent | priciest_order_id | priciest_amount |
|---|---|---|---|
| 170297 | 222,124,000 | 5654600 | 10,005,000 |
| 177251 | 218,872,000 | 2086420 | 9,700,000 |
| 99858 | 205,072,000 | 105337 | 9,865,000 |
| 90256 | 204,291,000 | 7705049 | 9,865,000 |
| 180940 | 204,270,000 | 5676406 | 9,830,000 |
| … (۱۰ ردیف در `out/05.txt`) | | | |

**۵.** ۳۶۵ ردیف. روز اول: درآمد `55,783,401,000`، تجمعی `55,783,401,000`، میانگین متحرک `55,783,401,000` (چون فقط یک روز موجود است). روز ۷ام (۲۰۲۵-۰۱-۰۷): میانگین متحرک `55,724,553,143`. روز آخر (۲۰۲۵-۱۲-۳۱): درآمد `55,266,007,000`، تجمعی `20,178,107,193,000`.

**۶.** (ژانویه، ماه اول، `change_pct` برابر `NULL`)

| month | city | revenue | rnk | prev_revenue | change_pct |
|---|---|---|---|---|---|
| 2025-01 | Tehran | 645,259,402,000 | 1 | 0 | NULL |
| 2025-01 | Mashhad | 218,417,429,000 | 2 | 0 | NULL |
| 2025-02 | Tehran | 583,809,469,000 | 1 | 645,259,402,000 | -9.52 |
| 2025-03 | Tehran | 645,149,679,000 | 1 | 583,809,469,000 | 10.51 |

**۷.**

| month | new_customers |
|---|---|
| 2025-01 | 197,184 |
| 2025-02 | 2,743 |
| 2025-03 | 70 |
| 2025-04 | 2 |

**توضیح‌ها:**
- (۱) برای هر محصول فقط یک‌بار `dictGet` صدا زده می‌شود و دسته از Dictionary می‌آید، بدون JOIN.
- (۲) `LIMIT 3 BY category` بعد از مرتب‌سازی بر اساس درآمد، سه ردیف اول هر دسته را نگه می‌دارد.
- (۳) هر شرط داخل `sumIf`/`countIf` است، پس همه‌چیز در یک پیمایش جدول به دست می‌آید.
- (۴) `argMax(order_id, amount)` شناسه‌ی سفارشی را برمی‌گرداند که بیشترین مبلغ را دارد.
- (۵) در ۶ روز اول میانگین متحرک روی تعداد روزهای موجود حساب می‌شود.
- (۶) برای ماه اول `lagInFrame` مقدار پیش‌فرض ۰ می‌دهد و `nullIf(prev, 0)` درصد تغییر را `NULL` می‌کند (تقسیم بر صفر نداریم). از `toInt64` استفاده شد تا تفاوت منفی (کاهش) در نوع بدون علامت سرریز نکند.
- (۷) هر مشتری حدود ۵۰ سفارش با تاریخ تصادفی دارد، پس اولین سفارشش تقریباً همیشه در ژانویه است. مجموع ستون ۱۹۹٬۹۹۹ است (۲۰۰ هزار منهای مشتری ۱۲۳۴۵ که قبل از ساخت جدول با Lightweight Delete حذف شده بود). این الگو به خاطر تصادفی بودن داده است.

---

## بخش ۶: کوئری کند و بهینه‌سازی (`sql/06_performance.sql`)

کوئری پشتیبانی (`customer_id = 4321`) روی `orders` همه‌ی ۱۲۲۳ Granule را می‌خواند (`EXPLAIN` در سؤال ۱ و زیر). **نکته‌ی اندازه‌گیری:** ClickHouse نتیجه‌ی فیلتر را در Query Condition Cache نگه می‌دارد، پس اجرای دوم و سوم با cache روشن فقط ۴۲۸٬۸۷۵ ردیف خواند و عددها خوش‌بینانه شد؛ برای مقایسه‌ی منصفانه با `use_query_condition_cache = 0` اندازه‌گیری کردم.

| | زمان اجرا | read_bytes | read_rows | Granule خوانده‌شده / کل |
|---|---|---|---|---|
| قبل از بهینه‌سازی (`orders`، سه اجرا: 751 / 724 / 767 ms) | **~747 ms** | 49.42 MiB | 10,000,000 | 1223 / 1223 |
| بعد از بهینه‌سازی (`orders_by_customer`، میانگین ۳ اجرا) | **~104 ms** | 3.84 MiB | 237,568 | 29 / 1221 |

(عدد «بعد» با cache روشن گرفته شد؛ چون Granuleها را ایندکس کلید رد می‌کند و `read_rows` دقیقاً ۲۹ × ۸۱۹۲ است، اثر cache در آن نیست. اگر خواستید، با `use_query_condition_cache = 0` دوباره اندازه بگیرید و عدد را اینجا عوض کنید.)

**توضیح بند ۲:** چون `customer_id` در `ORDER BY` جدول `orders` نیست، Sparse Index نمی‌تواند هیچ Granuleای را رد کند و `EXPLAIN` هر سه مرحله (Min-Max، Partition، PrimaryKey) را `Condition: true` و ۱۲۲۳ از ۱۲۲۳ نشان می‌دهد.

**توضیح بند ۳:** بدون دست زدن به `ORDER BY` جدول `orders`، جدول دوم `orders_by_customer` با `ORDER BY (customer_id, created_at)` ساختم که با Materialized View پر می‌شود (و داده‌ی قبلی با backfill). حالا PrimaryKey فقط ۲۹ Granule از ۱۲۲۱ (حدود ۲.۴٪) را نگه می‌دارد؛ نتیجه: حدود ۷ برابر سریع‌تر، ۱۳ برابر حجم کمتر و ۴۲ برابر ردیف کمتر. `Read type: InReverseOrder` در `EXPLAIN` یعنی `ORDER BY created_at DESC` بدون مرتب‌سازی جداگانه، با خواندن از آخر انجام می‌شود. هزینه: فضای دیسک حدود دو برابر برای این داده و کمی کند شدن هر INSERT، چون هر سطر در دو جدول نوشته می‌شود.

### بند ۴ — سه کوئری سنگین (بر اساس `read_bytes` از `system.query_log`)

| رتبه | کوئری | read_bytes | زمان |
|---|---|---|---|
| ۱ | ۱۰ مشتری برتر با `argMax` (بخش ۵، کوئری ۴) | 141.10 MiB | 1,522 ms |
| ۲ | JOIN با `products_pg` (سؤال ۱۱) | 83.96 MiB | 1,063 ms |
| ۳ | `uniqExact` ماهانه (سؤال ۱۰) | 83.88 MiB | 886 ms |

**پیشنهاد بهبود برای کوئری ۱:** این کوئری هر بار کل `orders` را می‌خواند و برای حدود ۲۰۰ هزار مشتری گروه‌بندی می‌کند، در حالی که جواب فقط به جمع و بیشینه‌ی هر مشتری وابسته است. یک جدول خلاصه با Materialized View ساختم:

```sql
CREATE TABLE shop.customer_spend (
    customer_id UInt32,
    total_spent SimpleAggregateFunction(sum, UInt64),
    top_amount  SimpleAggregateFunction(max, UInt64),
    top_order   AggregateFunction(argMax, UInt32, UInt64)
) ENGINE = AggregatingMergeTree ORDER BY customer_id;

CREATE MATERIALIZED VIEW shop.customer_spend_mv TO shop.customer_spend AS
SELECT customer_id, sum(toUInt64(quantity) * unit_price) AS total_spent,
       max(toUInt64(quantity) * unit_price) AS top_amount,
       argMaxState(order_id, toUInt64(quantity) * unit_price) AS top_order
FROM shop.orders WHERE status = 'delivered' GROUP BY customer_id;
-- + backfill با INSERT ... SELECT

SELECT customer_id, sum(total_spent) AS total_spent,
       argMaxMerge(top_order) AS priciest_order_id, max(top_amount) AS priciest_amount
FROM shop.customer_spend GROUP BY customer_id ORDER BY total_spent DESC LIMIT 10;
```

| | زمان | read_rows | read_bytes |
|---|---|---|---|
| کوئری اصلی (روی `orders`) | 1,522 ms | 10,000,000 | 141.10 MiB |
| بعد از بهبود (`customer_spend`) | **363 ms** | **199,999** | **9.92 MiB** |

نتیجه (۱۰ مشتری، `total_spent` و `priciest_order_id`) عیناً با کوئری اصلی یکی بود (مثلاً مشتری ۱۷۰۲۹۷ با ۲۲۲٬۱۲۴٬۰۰۰ اول است). حدود ۴.۲ برابر سریع‌تر، ۱۴ برابر حجم کمتر و ۵۰ برابر ردیف کمتر؛ زمان به اندازه‌ی حجم کم نشد چون هنوز برای ~۲۰۰ هزار مشتری `argMaxMerge` و مرتب‌سازی لازم است. **هزینه و محدودیت:** فضای کمی می‌گیرد و INSERT را کمی کندتر می‌کند؛ مثل بقیه‌ی MVها فقط INSERT را می‌بیند، پس `UPDATE` و `DELETE` بعدی روی `orders` در آن منعکس نمی‌شود.

---

## بخش اضافه: Kafka (`sql/07_kafka.sql`, `sql/08_kafka_checks.sql`, `scripts/kafka.sh`)

مراحل اجرا:
1. `COMPOSE_PROFILES=kafka docker compose up -d` (سرویس `kafka` با ایمیج `apache/kafka:4.0.0` بدون ZooKeeper)
2. `./scripts/kafka.sh topic` ← `Created topic orders.` (۳ پارتیشن)
3. `sql/07_kafka.sql`: جدول `orders_queue` (موتور Kafka، `JSONEachRow`، گروه `orders_clickhouse`، `kafka_handle_error_mode = 'stream'`)، `orders_queue_mv` (پیام‌های سالم با `length(_error) = 0` به `orders`)، و `orders_queue_errors` با MV دوم برای پیام‌های خراب (`_raw_message`, `_topic`, `_partition`, `_offset`, `_error`)
4. `./scripts/kafka.sh send`: ۱۰۰۰ سفارش جدید با `order_id` از ۲۰۰۰۰۰۰۰ و بعد یک پیام خراب (`this is not json`)
5. `sql/08_kafka_checks.sql` و `./scripts/kafka.sh describe`

### سؤال ۱۲ — سفارش‌ها و Offset

**کوئری:** `SELECT count() FROM shop.orders WHERE order_id >= 20000000`؛ `SELECT sum(orders) FROM shop.daily_sales WHERE day = toDate(now())`؛ `system.kafka_consumers`؛ `kafka-consumer-groups.sh --describe --group orders_clickhouse`.

**خروجی:**

- سفارش‌های جدید در `orders`: **1000**.
- `daily_sales` برای امروز: مجموع `orders` برابر **1000** (همه `delivered` و `Tehran`، پس `daily_sales_mv` هم از Kafka تغذیه شده است).
- از دید ClickHouse (`system.kafka_consumers`): پارتیشن‌های `[0, 1, 2]` با Offsetهای `[373, 442, 186]`.
- از دید Kafka (`kafka-consumer-groups.sh --describe`):

| PARTITION | CURRENT-OFFSET | LOG-END-OFFSET | LAG |
|---|---|---|---|
| 0 | 373 | 373 | **0** |
| 1 | 442 | 442 | **0** |
| 2 | 186 | 186 | **0** |

**توضیح:** مجموع Offsetها ۳۷۳ + ۴۴۲ + ۱۸۶ = ۱۰۰۱ است، یعنی ۱۰۰۰ سفارش خوب به‌علاوه‌ی ۱ پیام خراب. هر دو ابزار Offset یکسانی نشان می‌دهند و **Lag برابر صفر** است: ClickHouse همه‌ی پیام‌ها را (حتی پیام خراب) مصرف و commit کرده است. Lag یعنی `LOG-END-OFFSET − CURRENT-OFFSET`، تعداد پیام‌هایی که هنوز خوانده نشده‌اند.

### سؤال ۱۳ — پیام خراب

**کوئری:** `SELECT * FROM shop.orders_queue_errors`

**خروجی:**
```
raw_message:     this is not json
kafka_topic:     orders
kafka_partition: 0
kafka_offset:    372
error:           Cannot parse input: expected '{' before: 'this is not json': (at row 1)
received_at:     2026-11-10 09:39:28
```

**توضیح:** پیام خراب با متن اصلی، پارتیشن، Offset و متن خطا در جدول `orders_queue_errors` ذخیره شد (از طریق MV دوم) و به `orders` نرسید. به همین دلیل Lag بعد از آن هم صفر شد. اگر `kafka_handle_error_mode` را `'stream'` نکرده بودیم (حالت پیش‌فرض)، خطای parse مصرف را متوقف می‌کرد و پیام‌های بعدی پشت همان پیام خراب گیر می‌کردند؛ این حالت را اجرا نکردم و توضیحم بر اساس رفتار پیش‌فرض است.

### سؤال ۱۴ — ری‌استارت وسط مصرف

**توضیح:** (بر اساس رفتار Kafka Engine؛ اجرا نشده.) ClickHouse Kafka Engine مصرف را **at-least-once** انجام می‌دهد: Offset بعد از نوشتن داده در جدول‌های مقصد commit می‌شود. پس با ری‌استارت وسط کار پیام معمولاً گم نمی‌شود، ولی اگر قبل از commit قطع شود، همان دسته دوباره خوانده و دوبار وارد جدول می‌شود. برای اینکه سفارش تکراری در گزارش‌ها دوبار شمرده نشود، جدول مقصد باید `ReplacingMergeTree` (با کلید `order_id`) باشد و گزارش‌ها با `FINAL` یا `argMax` بخوانند. توجه: جدول‌های جمع‌شونده مثل `SummingMergeTree` تکرار را دوبار جمع می‌کنند، پس حذف تکرار باید قبل از آن‌ها انجام شود.

---

## کارهای امتیازی (`sql/09_bonus.sql`)

### CollapsingMergeTree — موجودی انبار

```sql
CREATE TABLE shop.stock (product_id UInt16, qty Int32, sign Int8)
ENGINE = CollapsingMergeTree(sign) ORDER BY product_id;

INSERT INTO shop.stock SELECT 1, 100, 1;                 -- موجودی اولیه: ۱۰۰
INSERT INTO shop.stock SELECT * FROM values('product_id UInt16, qty Int32, sign Int8',
                                            (1, 100, -1), (1, 80, 1));   -- تغییر به ۸۰: ابطال قبلی + ثبت جدید

SELECT * FROM shop.stock ORDER BY sign, qty;
SELECT product_id, sum(qty * sign) AS current_qty FROM shop.stock GROUP BY product_id HAVING sum(sign) > 0;
SELECT * FROM shop.stock FINAL;
```

**خروجی:**

ردیف‌های خام (تغییر موجودی به‌صورت «ابطال + ثبت» ذخیره شده است):

| product_id | qty | sign |
|---|---|---|
| 1 | 100 | -1 |
| 1 | 80 | 1 |
| 1 | 100 | 1 |

موجودی فعلی با `sum(qty * sign) ... HAVING sum(sign) > 0`: `product_id = 1`، `current_qty = 80`.

با `FINAL`: فقط ردیف `(1, 80, 1)` می‌ماند.

**توضیح:** برای تغییر موجودی ردیف قبلی را نمی‌شود دست زد؛ ردیفی با `sign = -1` و همان مقدار قبلی (ابطال) و ردیف جدید با `sign = 1` ثبت می‌شود. `CollapsingMergeTree` این جفت‌ها را هنگام ادغام حذف می‌کند، ولی چون زمان ادغام قطعی نیست، برای خواندن درست یا `sum(qty * sign)` با `HAVING sum(sign) > 0` یا `FINAL` لازم است (مثل `SummingMergeTree` در سؤال ۹).

### موتور Merge

```sql
CREATE TABLE shop.orders_2024 AS shop.orders
ENGINE = MergeTree PARTITION BY toYYYYMM(created_at) ORDER BY (city, created_at);
-- ۱ میلیون سفارش سال ۲۰۲۴ (INSERT ... SELECT ... FROM numbers(1000000)، order_id از 100000000 شروع می‌شود)

CREATE TABLE shop.orders_all AS shop.orders ENGINE = Merge('shop', '^orders(_2024)?$');

SELECT _table, toYear(created_at) AS year, count() AS orders
FROM shop.orders_all GROUP BY _table, year ORDER BY year;
```

**خروجی:**

| _table | year | orders |
|---|---|---|
| orders_2024 | 2024 | 1,000,000 |
| orders | 2025 | 9,999,953 |

**توضیح:** `orders_all` خودش داده ندارد و فقط جدول‌هایی از دیتابیس `shop` را که اسمشان با regex جور باشد (`orders` و `orders_2024`) در یک کوئری نشان می‌دهد؛ ستون مجازی `_table` می‌گوید هر ردیف از کدام جدول آمده. regex را طوری نوشتم که `orders_naive` و `orders_by_customer` را شامل نشود. تعداد سفارش‌های ۲۰۲۵ ۹٬۹۹۹٬۹۵۳ است: ۱۰ میلیون منهای ۴۷ سفارش مشتری ۱۲۳۴۵ که در بخش ۳ با Lightweight Delete حذف شد.

### MinIO / S3 — اجرا نشد

سرویس `minio` در compose (پشت profile `bonus`) و فایل `sql/10_minio.sql` (نوشتن و خواندن Parquet با `s3()`) آماده است، ولی دانلود ایمیج `minio/minio` از Docker Hub روی سیستم من `pull access denied` داد، پس این کار اجرا نشد.