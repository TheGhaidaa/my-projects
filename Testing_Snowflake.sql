-- customer dim
-- checking for customer_id is not null // True means there is no null values
select count(*) = 0 as null_check from sf_tpcds.analytics.customer_dim
where c_customer_sk is null;

-- *******************************

-- Weekly sales inventory
-- testing for warehouse_sk, item_sk and sold_wk_sk are unique
select count(*) = 0 as unique_check from
    (select
        warehouse_sk, item_sk, sold_wk_sk
    from SF_TPCDS.ANALYTICS.WEEKLY_SALES_INVENTORY
    group by 1,2,3
    having count(*) > 1);

-- *******************************

-- relationship test
select count(*) = 0 as relationship_test from
    (select
        dim.i_item_sk
    from SF_TPCDS.ANALYTICS.WEEKLY_SALES_INVENTORY fact
    left join TPCDS.RAW.ITEM dim
    on dim.i_item_sk = fact.item_sk
    where dim.i_item_sk is null);

-- *******************************

-- Accepted value test  // no warehouse_sk is less than 1 and greater than 6
select count(*) = 0 as Accepted_value_test from SF_TPCDS.ANALYTICS.WEEKLY_SALES_INVENTORY
where warehouse_sk not in (1,2,3,4,5,6);