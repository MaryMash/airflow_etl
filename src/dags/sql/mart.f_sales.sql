with user_order_status_log as(
select date_time,
       item_id, 
       customer_id,
       city_id, 
       quantity,
       status,
       case 
       	when status = 'refunded' then payment_amount * -1
       	else payment_amount
       end as payment_amount 
from staging.user_order_log uol
)
insert into mart.f_sales (date_id, item_id, customer_id, city_id, quantity, payment_amount)
select dc.date_id, item_id, customer_id, city_id, quantity, payment_amount from user_order_status_log uosl
left join mart.d_calendar as dc on uosl.date_time::Date = dc.date_actual
where uosl.date_time::Date = '{{ds}}';