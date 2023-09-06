INSERT INTO mart.d_item (item_id, item_name)
SELECT item_id,
       item_name
FROM staging.user_order_log
WHERE item_id not in
    (SELECT item_id
     FROM mart.d_item)
GROUP BY item_id,
         item_name;