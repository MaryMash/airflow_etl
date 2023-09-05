CREATE TABLE staging.user_order_log (
	uniq_id varchar(32) NOT NULL,
	date_time timestamp NOT NULL,
	city_id int NOT NULL,
	city_name varchar(100) NULL,
	customer_id int NOT NULL,
	first_name varchar(100) NULL,
	last_name varchar(100) NULL,
	item_id int NOT NULL,
	item_name varchar(100) NULL,
	quantity bigint NULL,
	payment_amount numeric(10, 2) NULL,
	status varchar(100) NULL,
	CONSTRAINT user_order_log_pk PRIMARY KEY (uniq_id)
);
CREATE INDEX uo1 ON staging.user_order_log USING btree (customer_id);
CREATE INDEX uo2 ON staging.user_order_log USING btree (item_id);


CREATE TABLE IF NOT EXISTS staging.user_activity_log(
    uniq_id varchar(32) NOT NULL,
    date_time timestamp NOT NULL,
    action_id int NOT NULL,
    customer_id int NOT NULL,
    quantity bigint,
    CONSTRAINT user_activity_log_pk PRIMARY KEY (uniq_id)
);

CREATE TABLE IF NOT EXISTS staging.customer_research(
	date_id timestamp,
	category_id int,
	geo_id int,
	sales_qty bigint,
	sales_amt numeric(14, 2)
);

CREATE TABLE IF NOT EXISTS staging.price_log(
    product_name varchar(100) NOT NULL,
    price numeric(10, 2) NOT NULL,
    CONSTRAINT price_log_pk PRIMARY KEY (product_name, price)
);
