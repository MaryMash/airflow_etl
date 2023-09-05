CREATE OR REPLACE VIEW mart.f_customer_retention AS
WITH orders_count AS
  (SELECT extract('week'
                  FROM uol.date_time) AS period_id,
          customer_id,
          item_id,
          count(uniq_id) order_count,
          CASE
              WHEN count(uniq_id) > 1 THEN 'returning'
              ELSE 'new'
          END client_type,
          sum(CASE
                  WHEN uol.status = 'refunded' THEN 1
                  ELSE 0
              END) AS refunded_orders,
          sum(payment_amount) AS payment_amount
   FROM
     (SELECT uniq_id,
             date_time,
             item_id,
             customer_id,
             status,
             CASE
                 WHEN status = 'refunded' THEN payment_amount * -1
                 ELSE payment_amount
             END AS payment_amount
      FROM staging.user_order_log uol_2) AS uol
   GROUP BY 1,
            2,
            3)
SELECT sum(CASE
               WHEN order_count = 1 THEN 1
               ELSE 0
           END) AS new_customers_count,
       sum(CASE
               WHEN order_count > 1 THEN 1
               ELSE 0
           END) AS returning_customers_count,
       sum(CASE
               WHEN refunded_orders > 0 THEN 1
               ELSE 0
           END) AS refunded_customer_count,
       'weekly' AS period_name,
       period_id,
       item_id,
       sum(CASE
               WHEN client_type = 'new' THEN payment_amount
               ELSE 0
           END) new_customers_revenue,
       sum(CASE
               WHEN client_type = 'returning' THEN payment_amount
               ELSE 0
           END) returning_customers_revenue,
       sum(refunded_orders) AS customers_refunded
FROM orders_count
GROUP BY 4,
         5,
         6;