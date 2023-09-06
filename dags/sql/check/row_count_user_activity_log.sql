SELECT count(distinct(customer_id)) > 3
FROM staging.user_activity_log
WHERE date_time = '{{ds}}';