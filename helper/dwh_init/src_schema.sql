
CREATE TABLE customers (
    customer_id varchar(50),
    customer_name varchar(100),
    address varchar(255),
    city varchar(75),
    province varchar(75),
    created_at timestamptz DEFAULT CURRENT_TIMESTAMP,
    update_at timestamptz DEFAULT CURRENT_TIMESTAMP

);
ALTER TABLE customers
add CONSTRAINT unique_customer_id unique (customer_id);


CREATE TABLE delivery_man (
    deliveryman_id int NOT NULL,
    deliveryman_name varchar(100) NOT NULL,
    contact varchar(75),
    created_at timestamptz DEFAULT CURRENT_TIMESTAMP,
    update_at timestamptz DEFAULT CURRENT_TIMESTAMP
);

ALTER TABLE delivery_man
add CONSTRAINT unique_delivery_man_id unique (deliveryman_id);

CREATE TABLE expedition (
    expedition_id int NOT NULL,
    expedition_name varchar(75) NOT NULL,
    contact varchar(75),
    created_at timestamptz DEFAULT CURRENT_TIMESTAMP,
    update_at timestamptz DEFAULT CURRENT_TIMESTAMP
);

ALTER TABLE expedition
ADD CONSTRAINT unique_expedition_id unique (expedition_id);

CREATE TABLE license_plate (
    license_id int NOT NULL,
    licenseplate_number varchar(25),
    owner_name varchar(10),
    created_at timestamptz DEFAULT CURRENT_TIMESTAMP,
    update_at timestamptz DEFAULT CURRENT_TIMESTAMP
);

ALTER TABLE license_plate
ADD CONSTRAINT unique_license_plate_id unique(license_id);

CREATE TABLE office_address (
    id_address int NOT NULL,
    code varchar(10),
    region varchar(25),
    address varchar(355),
    created_at timestamptz DEFAULT CURRENT_TIMESTAMP,
    update_at timestamptz DEFAULT CURRENT_TIMESTAMP
);

ALTER TABLE office_address
ADD CONSTRAINT unique_office_address_id unique (id_address);


CREATE TABLE packinglist (
    packinglist_id int,
    code varchar(10),
    no_packinglist varchar(50),
    sj_numbers text,
    packinglist_time timestamp,
    origin varchar(25),
    destination varchar(50),
    delivery_man varchar(100),
    recipient_address text,
    expedition varchar(75),
    awb varchar(30),
    expedition_fee int,
    total_pl int,
    notes text,
    src_created_at timestamp,
    src_update_at timestamp,
    created_at timestamptz DEFAULT CURRENT_TIMESTAMP,
    update_at timestamptz DEFAULT CURRENT_TIMESTAMP

);

ALTER TABLE packinglist
ADD CONSTRAINT unique_packinglist_id unique (packinglist_id);

CREATE TABLE transferstatus (
    transfer_id int NOT NULL,
    no_packinglist varchar(50),
    sj_numbers text,
    destination varchar(50),
    expedition varchar(75),
    delivery_man varchar(100),
    status_confirm varchar(15),
    confirm_date timestamp DEFAULT NULL,
    notes text,
    src_created_at timestamp,
    src_update_at timestamp,
    created_at timestamptz DEFAULT CURRENT_TIMESTAMP,
    update_at timestamptz DEFAULT CURRENT_TIMESTAMP
);

ALTER TABLE transferstatus
add CONSTRAINT unique_transfer_status_id unique (transfer_id);

CREATE TABLE deliveryproof (
    no_id int NOT NULL,
    code varchar(10),
    no_sj varchar(255),
    delivery_status varchar(20),
    reason varchar(100),
    delivery_man varchar(100),
    license_plate varchar(25),
    expedition varchar(100),
    expedition_fee int,
    total_payment int,
    pod_date timestamp,
    notes text,
    pod_image text,
    src_created_at timestamp,
    src_update_at timestamp,
    created_at timestamptz DEFAULT CURRENT_TIMESTAMP,
    update_at timestamptz DEFAULT CURRENT_TIMESTAMP
);

ALTER TABLE deliveryproof
ADD CONSTRAINT unique_deliveryproof_id UNIQUE (no_id);

