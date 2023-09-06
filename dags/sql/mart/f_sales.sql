WITH user_order_status_log AS
  (SELECT date_time,
          item_id,
          customer_id,
          city_id,
          quantity,
          status,
          CASE
              WHEN status = 'refunded' THEN payment_amount * -1
              ELSE payment_amount
          END AS payment_amount
   FROM staging.user_order_log uol)
INSERT INTO mart.f_sales (date_id, item_id, customer_id, city_id, quantity, payment_amount)
SELECT dc.date_id,
       item_id,
       customer_id,
       city_id,
       quantity,
       payment_amount
FROM user_order_status_log uosl
LEFT JOIN mart.d_calendar AS dc ON uosl.date_time::Date = dc.date_actual
WHERE uosl.date_time::Date = '{{ds}}';