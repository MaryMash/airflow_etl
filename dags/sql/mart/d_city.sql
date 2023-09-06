INSERT INTO mart.d_city (city_id, city_name)
SELECT city_id,
       city_name
FROM staging.user_order_log
WHERE city_id not in
    (SELECT city_id
     FROM mart.d_city)
GROUP BY city_id,
         city_name;