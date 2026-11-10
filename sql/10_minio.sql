-- کار امتیازی اختیاری: MinIO / S3 (نیازمند: COMPOSE_PROFILES=bonus docker compose up -d و ساختن bucket به نام shop از کنسول http://localhost:9001)
INSERT INTO FUNCTION s3('http://minio:9000/shop/orders_202503.parquet', 'minioadmin', 'minioadmin', 'Parquet')
SELECT * FROM shop.orders WHERE toYYYYMM(created_at) = 202503;

SELECT count() AS rows_from_s3
FROM s3('http://minio:9000/shop/orders_202503.parquet', 'minioadmin', 'minioadmin', 'Parquet');

