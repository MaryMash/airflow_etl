--d_calendar
CREATE TABLE mart.d_calendar (
	date_id int4 NOT NULL,
	date_actual date NOT NULL,
	epoch int8 NOT NULL,
	day_suffix varchar(4) NOT NULL,
	day_name varchar(9) NOT NULL,
	day_of_week int4 NOT NULL,
	day_of_month int4 NOT NULL,
	day_of_quarter int4 NOT NULL,
	day_of_year int4 NOT NULL,
	week_of_month int4 NOT NULL,
	week_of_year int4 NOT NULL,
	week_of_year_iso bpchar(10) NOT NULL,
	month_actual int4 NOT NULL,
	month_name varchar(9) NOT NULL,
	month_name_abbreviated bpchar(3) NOT NULL,
	quarter_actual int4 NOT NULL,
	quarter_name varchar(9) NOT NULL,
	year_actual int4 NOT NULL,
	first_day_of_week date NOT NULL,
	last_day_of_week date NOT NULL,
	first_day_of_month date NOT NULL,
	last_day_of_month date NOT NULL,
	first_day_of_quarter date NOT NULL,
	last_day_of_quarter date NOT NULL,
	first_day_of_year date NOT NULL,
	last_day_of_year date NOT NULL,
	mmyyyy bpchar(6) NOT NULL,
	mmddyyyy bpchar(10) NOT NULL,
	weekend_indr bool NOT NULL,
	CONSTRAINT d_date_date_dim_id_pk PRIMARY KEY (date_id)
);
CREATE INDEX d_date_date_actual_idx ON mart.d_calendar USING btree (date_actual);

--d_city
CREATE TABLE mart.d_city (
	id serial4 NOT NULL,
	city_id int4 NULL,
	city_name varchar(50) NULL,
	CONSTRAINT d_city_city_id_key UNIQUE (city_id),
	CONSTRAINT d_city_pkey PRIMARY KEY (id)
);
CREATE INDEX d_city1 ON mart.d_city USING btree (city_id);

--d_customer
CREATE TABLE mart.d_customer (
	id serial4 NOT NULL,
	customer_id int4 NOT NULL,
	first_name varchar(15) NULL,
	last_name varchar(15) NULL,
	city_id int4 NULL,
	CONSTRAINT d_customer_customer_id_key UNIQUE (customer_id),
	CONSTRAINT d_customer_pkey PRIMARY KEY (id)
);
CREATE INDEX d_cust1 ON mart.d_customer USING btree (customer_id);
ALTER TABLE mart.d_customer ADD CONSTRAINT d_customer_city_id_fkey FOREIGN KEY (city_id) REFERENCES mart.d_city(city_id);

--d_item
CREATE TABLE mart.d_item (
	id serial4 NOT NULL,
	item_id int4 NOT NULL,
	item_name varchar(50) NULL,
	CONSTRAINT d_item_item_id_key UNIQUE (item_id),
	CONSTRAINT d_item_pkey PRIMARY KEY (id)
);
CREATE UNIQUE INDEX d_item1 ON mart.d_item USING btree (item_id);


--f_sales
CREATE TABLE mart.f_sales (
	id serial4 NOT NULL,
	date_id int4 NOT NULL,
	item_id int4 NOT NULL,
	customer_id int4 NOT NULL,
	city_id int4 NOT NULL,
	quantity int8 NULL,
	payment_amount numeric(10, 2) NULL,
	CONSTRAINT f_sales_pkey PRIMARY KEY (id)
);
CREATE INDEX f_ds1 ON mart.f_sales USING btree (date_id);
CREATE INDEX f_ds2 ON mart.f_sales USING btree (item_id);
CREATE INDEX f_ds3 ON mart.f_sales USING btree (customer_id);
CREATE INDEX f_ds4 ON mart.f_sales USING btree (city_id);

ALTER TABLE mart.f_sales ADD CONSTRAINT f_sales_city_id_fkey FOREIGN KEY (city_id) REFERENCES mart.d_city(city_id);
ALTER TABLE mart.f_sales ADD CONSTRAINT f_sales_customer_id_fkey FOREIGN KEY (customer_id) REFERENCES mart.d_customer(customer_id);
ALTER TABLE mart.f_sales ADD CONSTRAINT f_sales_date_id_fkey FOREIGN KEY (date_id) REFERENCES mart.d_calendar(date_id);
ALTER TABLE mart.f_sales ADD CONSTRAINT f_sales_item_id_fkey FOREIGN KEY (item_id) REFERENCES mart.d_item(item_id);
