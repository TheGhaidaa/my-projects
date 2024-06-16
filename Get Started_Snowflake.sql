create or replace database TPCDS;
create or replace schema RAW;

create or replace table TPCDS.RAW.inventory (
inv_date_sk int not null,
inv_item_sk int not null,
inv_quantity_on_hand int,
inv_warehouse_sk int not null
);


-- create user
create or replace user wcdsnow
password = 'Wcd123456';

grant role accountadmin to user wcdsnow;

-- drop user
-- drop user wcdsnow;