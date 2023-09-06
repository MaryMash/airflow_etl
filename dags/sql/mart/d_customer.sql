-- обновление данных по новым клиентам
WITH last_city AS
  (SELECT customer_id,
          date_time,
          city_id,
          row_number () OVER (PARTITION BY customer_id
                              ORDER BY date_time DESC, city_id DESC) AS "rank"
   FROM staging.user_order_log)
INSERT  INTO mart.d_customer (customer_id, first_name, last_name, city_id)
SELECT DISTINCT l.customer_id,
       l.first_name,
       l.last_name,
       r.city_id
FROM staging.user_order_log l
LEFT JOIN
  (SELECT customer_id,
          city_id,
          "rank"
   FROM last_city
   WHERE "rank" = 1 ) r ON l.customer_id = r.customer_id
WHERE l.customer_id NOT IN
    (SELECT customer_id
     FROM mart.d_customer);

-- обновление данных по вернувшимся клиентам
UPDATE mart.d_customer b
SET city_id = a.city_id
FROM staging.user_order_log a
WHERE a.customer_id = b.customer_id
  AND a.date_time::Date = '{{ds}}';