CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE SCHEMA IF NOT EXISTS dwh_demo;

DROP TABLE IF EXISTS dwh_demo.dim_leadtime_area;
CREATE TABLE dwh_demo.dim_leadtime_area (
  leadtime_area_id uuid PRIMARY KEY,
  area_name varchar(100),
  day_of_leadtime int
);

DROP TABLE IF EXISTS dwh_demo.dim_customers;
CREATE TABLE dwh_demo.dim_customers (
  customer_id uuid DEFAULT uuid_generate_v4() PRIMARY KEY,
  nk_customer_id varchar(50),
  leadtime_area_id uuid,
  customer_name varchar(100),
  city varchar(75),
  province varchar(75),
  branch varchar(50),
  created_at timestamp DEFAULT CURRENT_TIMESTAMP,
  update_at timestamp DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT fk_leadtime_area FOREIGN KEY (leadtime_area_id) REFERENCES dwh_demo.dim_leadtime_area (leadtime_area_id)
);

DROP TABLE IF EXISTS dwh_demo.dim_date;
CREATE TABLE dwh_demo.dim_date (
  date_id int PRIMARY KEY NOT NULL,
  date_actual timestamp NOT NULL,
  day_suffix varchar(4) NOT NULL,
  day_name varchar(9) NOT NULL,
  day_of_year int NOT NULL,
  week_of_month int NOT NULL,
  week_of_year int NOT NULL,
  month_actual int NOT NULL,
  month_name varchar(9) NOT NULL,
  month_name_abbreviated char(3) NOT NULL,
  quarter_actual int NOT NULL,
  quarter_name varchar(9) NOT NULL,
  year_actual int NOT NULL,
  is_weekend BOOLEAN NOT NULL,
  is_holiday BOOLEAN DEFAULT FALSE,
  is_working_day BOOLEAN

);

CREATE INDEX dim_date_actual_idx
ON dwh_demo.dim_date(date_actual);

DROP TABLE IF EXISTS dwh_demo.dim_time;
CREATE TABLE dwh_demo.dim_time (
	time_id int PRIMARY KEY NOT NULL,
	time_actual time NOT NULL,
	hours_24 character(2) NOT NULL,
	hours_12 character(2) NOT NULL,
	hour_minutes character (2)  NOT NULL,
	day_minutes integer NOT NULL,
	day_time_name character varying (20) NOT NULL,
	day_night character varying (20) NOT NULL
);

DROP TABLE IF EXISTS dwh_demo.dim_deliveryman;
CREATE TABLE dwh_demo.dim_deliveryman (
  man_id uuid DEFAULT uuid_generate_v4() PRIMARY KEY,
  delivery_man_name varchar(100) NOT NULL,
  area varchar(50),
  created_at timestamp DEFAULT CURRENT_TIMESTAMP,
  update_at timestamp DEFAULT CURRENT_TIMESTAMP
);

DROP TABLE IF EXISTS dwh_demo.dim_expeditions;
CREATE TABLE dwh_demo.dim_expeditions (
  expedition_id uuid DEFAULT uuid_generate_v4() PRIMARY KEY,
  expediton_name varchar(100),
  created_at timestamp DEFAULT CURRENT_TIMESTAMP,
  update_at timestamp DEFAULT CURRENT_TIMESTAMP
);

DROP TABLE IF EXISTS dwh_demo.dim_office;
CREATE TABLE dwh_demo.dim_office (
  office_id uuid DEFAULT uuid_generate_v4() PRIMARY KEY,
  office_name varchar(100),
  code varchar(4),
  address_nanme text,
  created_at timestamp DEFAULT CURRENT_TIMESTAMP,
  update_at timestamp DEFAULT CURRENT_TIMESTAMP
);

DROP TABLE IF EXISTS dwh_demo.dim_license_plate;
CREATE TABLE dwh_demo.dim_license_plate (
  license_id uuid DEFAULT uuid_generate_v4() PRIMARY KEY,
  license_name varchar(50),
  created_at timestamp DEFAULT CURRENT_TIMESTAMP,
  update_at timestamp DEFAULT CURRENT_TIMESTAMP
);

DROP TABLE IF EXISTS dwh_demo.dim_fct_monitoring;
CREATE TABLE dwh_demo.fct_monitoring (
  order_id uuid DEFAULT uuid_generate_v4() PRIMARY KEY,
  nk_order_id uuid,
  no_po varchar(50),
  no_so varchar(50) NOT NULL,
  no_sj varchar(50),
  customer_id uuid,
  total_order float,
  no_packinglist varchar(50),
  so_date int NOT NULL,
  so_time int NOT NULL,
  sj_date int,
  sj_time int,
  po_expired_date int,
  packinglist_date int,
  packinglist_time int,
  man_id uuid,
  expedition_id uuid,
  origin_id uuid,
  destination_id uuid,
  status_confirm varchar(50),
  confirm_date int,
  confirm_time int,
  delivery_status varchar(50),
  pod_date int,
  pod_time int,
  license_id uuid,
  reason varchar(100),
  notes_pod text,
  created_at timestamp DEFAULT CURRENT_TIMESTAMP,
  update_at timestamp DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT fk_customer_id FOREIGN KEY (customer_id) REFERENCES dwh_demo.dim_customers (customer_id),
  CONSTRAINT fk_po_expired_date FOREIGN KEY (po_expired_date) REFERENCES dwh_demo.dim_date (date_id),
  CONSTRAINT fk_so_date FOREIGN KEY (so_date) REFERENCES dwh_demo.dim_date (date_id),
  CONSTRAINT fk_so_time FOREIGN KEY (so_time) REFERENCES dwh_demo.dim_time (time_id),
  CONSTRAINT fk_sj_date FOREIGN KEY (sj_date) REFERENCES dwh_demo.dim_date (date_id),
  CONSTRAINT fk_sj_time FOREIGN KEY (sj_time) REFERENCES dwh_demo.dim_time (time_id),
  CONSTRAINT fk_packinglist_date FOREIGN KEY (packinglist_date) REFERENCES dwh_demo.dim_date (date_id),
  CONSTRAINT fk_packinglist_time FOREIGN KEY (packinglist_time) REFERENCES dwh_demo.dim_time(time_id),
  CONSTRAINT fk_confirm_date FOREIGN KEY (confirm_date) REFERENCES dwh_demo.dim_date (date_id),
  CONSTRAINT fk_confirm_time FOREIGN KEY (confirm_time) REFERENCES dwh_demo.dim_time (time_id),
  CONSTRAINT fk_pod_date FOREIGN KEY (pod_date) REFERENCES dwh_demo.dim_date (date_id),
  CONSTRAINT fk_pod_time FOREIGN KEY (pod_time) REFERENCES dwh_demo.dim_time (time_id),
  CONSTRAINT fk_delivery_man_id FOREIGN key (man_id) REFERENCES dwh_demo.dim_deliveryman (man_id),
  CONSTRAINT fk_expedition_id FOREIGN KEY (expedition_id) REFERENCES dwh_demo.dim_expeditions (expedition_id),
  CONSTRAINT fk_origin_id FOREIGN KEY (origin_id) REFERENCES dwh_demo.dim_office (office_id),
  CONSTRAINT fk_destination_id FOREIGN KEY (destination_id) REFERENCES dwh_demo.dim_office (office_id),
  CONSTRAINT fk_license_id FOREIGN KEY (license_id) REFERENCES dwh_demo.dim_license_plate (license_id)
);


INSERT INTO dwh_demo.dim_date
SELECT TO_CHAR(datum, 'yyyymmdd')::INT AS date_id,
       datum AS date_actual,
       TO_CHAR(datum, 'fmDDth') AS day_suffix,
       TO_CHAR(datum, 'TMDay') AS day_name,
       EXTRACT(DOY FROM datum) AS day_of_year,
       TO_CHAR(datum, 'W')::INT AS week_of_month,
       EXTRACT(WEEK FROM datum) AS week_of_year,
       EXTRACT(MONTH FROM datum) AS month_actual,
       TO_CHAR(datum, 'TMMonth') AS month_name,
       TO_CHAR(datum, 'Mon') AS month_name_abbreviated,
       EXTRACT(QUARTER FROM datum) AS quarter_actual,
       CASE
           WHEN EXTRACT(QUARTER FROM datum) = 1 THEN 'First'
           WHEN EXTRACT(QUARTER FROM datum) = 2 THEN 'Second'
           WHEN EXTRACT(QUARTER FROM datum) = 3 THEN 'Third'
           WHEN EXTRACT(QUARTER FROM datum) = 4 THEN 'Fourth'
           END AS quarter_name,
       EXTRACT(YEAR FROM datum) AS year_actual,
       CASE
          WHEN EXTRACT(DOW FROM datum) IN (0, 6) THEN TRUE
          ELSE FALSE END AS is_weekend,
        FALSE AS is_holiday,
        NULL AS is_working_day
FROM (SELECT '2015-01-01'::DATE + SEQUENCE.DAY AS datum
      FROM GENERATE_SERIES(0, 29219) AS SEQUENCE (DAY)
      GROUP BY SEQUENCE.DAY) DQ
ORDER BY 1;


INSERT INTO dwh_demo.dim_time

SELECT  
	cast(to_char(minute, 'hh24mi') as numeric) time_id,
	to_char(minute, 'hh24:mi')::time AS time_actual,
	-- Hour of the day (0 - 23)
	to_char(minute, 'hh24') AS hour_24,
	-- Hour of the day (0 - 11)
	to_char(minute, 'hh12') hour_12,
	-- Hour minute (0 - 59)
	to_char(minute, 'mi') hour_minutes,
	-- Minute of the day (0 - 1439)
	extract(hour FROM minute)*60 + extract(minute FROM minute) day_minutes,
	-- Names of day periods
	case 
		when to_char(minute, 'hh24:mi') BETWEEN '00:00' AND '11:59'
		then 'AM'
		when to_char(minute, 'hh24:mi') BETWEEN '12:00' AND '23:59'
		then 'PM'
	end AS day_time_name,
	-- Indicator of day or night
	case 
		when to_char(minute, 'hh24:mi') BETWEEN '07:00' AND '19:59' then 'Day'	
		else 'Night'
	end AS day_night
FROM 
	(SELECT '0:00'::time + (sequence.minute || ' minutes')::interval AS minute 
	FROM  generate_series(0,1439) AS sequence(minute)
GROUP BY sequence.minute
) DQ
ORDER BY 1;


ALTER TABLE dwh_demo.fct_monitoring
ADD CONSTRAINT fct_monitoring_unique UNIQUE (nk_order_id, no_so);

ALTER TABLE dwh_demo.dim_customers
ADD CONSTRAINT dim_customers_unique UNIQUE (nk_customer_id);