-- Task1: customer dimensions
CREATE OR REPLACE PROCEDURE SF_TPCDS.ANALYTICS.populating_customer_dimension_using_scd_type_2()
  RETURNS VARCHAR
  LANGUAGE SQL
  AS
  $$
  BEGIN
   MERGE INTO SF_TPCDS.INTERMEDIATE.CUSTOMER_SNAPSHOT t1
USING TPCDS.RAW.CUSTOMER t2
ON  t1.C_SALUTATION=t2.C_SALUTATION
    AND t1.C_PREFERRED_CUST_FLAG=t2.C_PREFERRED_CUST_FLAG 
    AND coalesce(t1.C_FIRST_SALES_DATE_SK, 0) = coalesce(t2.C_FIRST_SALES_DATE_SK,0) 
    AND t1.C_CUSTOMER_SK=t2.C_CUSTOMER_SK
    AND t1.C_LOGIN=t2.C_LOGIN
    AND coalesce(t1.C_CURRENT_CDEMO_SK,0) = coalesce(t2.C_CURRENT_CDEMO_SK,0)
    AND t1.C_FIRST_NAME=t2.C_FIRST_NAME
    AND coalesce(t1.C_CURRENT_HDEMO_SK,0) = coalesce(t2.C_CURRENT_HDEMO_SK,0)
    AND t1.C_CURRENT_ADDR_SK=t2.C_CURRENT_ADDR_SK
    AND t1.C_LAST_NAME=t2.C_LAST_NAME
    AND t1.C_CUSTOMER_ID=t2.C_CUSTOMER_ID
    AND coalesce(t1.C_LAST_REVIEW_DATE_SK,0) = coalesce(t2.C_LAST_REVIEW_DATE_SK,0)
    AND coalesce(t1.C_BIRTH_MONTH,0) = coalesce(t2.C_BIRTH_MONTH,0)
    AND t1.C_BIRTH_COUNTRY = t2.C_BIRTH_COUNTRY
    AND coalesce(t1.C_BIRTH_YEAR,0) = coalesce(t2.C_BIRTH_YEAR,0)
    AND coalesce(t1.C_BIRTH_DAY,0) = coalesce(t2.C_BIRTH_DAY,0)
    AND t1.C_EMAIL_ADDRESS = t2.C_EMAIL_ADDRESS
    AND coalesce(t1.C_FIRST_SHIPTO_DATE_SK,0) = coalesce(t2.C_FIRST_SHIPTO_DATE_SK,0)
WHEN NOT MATCHED 
THEN INSERT (
    C_SALUTATION, 
    C_PREFERRED_CUST_FLAG, 
    C_FIRST_SALES_DATE_SK, 
    C_CUSTOMER_SK, C_LOGIN, 
    C_CURRENT_CDEMO_SK, 
    C_FIRST_NAME, 
    C_CURRENT_HDEMO_SK, 
    C_CURRENT_ADDR_SK, 
    C_LAST_NAME, 
    C_CUSTOMER_ID, 
    C_LAST_REVIEW_DATE_SK, 
    C_BIRTH_MONTH, 
    C_BIRTH_COUNTRY, 
    C_BIRTH_YEAR, 
    C_BIRTH_DAY, 
    C_EMAIL_ADDRESS, 
    C_FIRST_SHIPTO_DATE_SK,
    START_DATE,
    END_DATE)
VALUES (
    t2.C_SALUTATION, 
    t2.C_PREFERRED_CUST_FLAG, 
    t2.C_FIRST_SALES_DATE_SK, 
    t2.C_CUSTOMER_SK, 
    t2.C_LOGIN, 
    t2.C_CURRENT_CDEMO_SK, 
    t2.C_FIRST_NAME, 
    t2.C_CURRENT_HDEMO_SK, 
    t2.C_CURRENT_ADDR_SK, 
    t2.C_LAST_NAME, 
    t2.C_CUSTOMER_ID, 
    t2.C_LAST_REVIEW_DATE_SK, 
    t2.C_BIRTH_MONTH, 
    t2.C_BIRTH_COUNTRY, 
    t2.C_BIRTH_YEAR, 
    t2.C_BIRTH_DAY, 
    t2.C_EMAIL_ADDRESS, 
    t2.C_FIRST_SHIPTO_DATE_SK,
    CURRENT_DATE(),
    NULL
);

MERGE INTO SF_TPCDS.INTERMEDIATE.CUSTOMER_SNAPSHOT t1
USING TPCDS.RAW.CUSTOMER t2
ON  t1.C_CUSTOMER_SK=t2.C_CUSTOMER_SK
WHEN MATCHED
    AND (
    t1.C_SALUTATION!=t2.C_SALUTATION
    OR t1.C_PREFERRED_CUST_FLAG!=t2.C_PREFERRED_CUST_FLAG 
    OR coalesce(t1.C_FIRST_SALES_DATE_SK, 0) != coalesce(t2.C_FIRST_SALES_DATE_SK,0) 
    OR t1.C_LOGIN!=t2.C_LOGIN
    OR coalesce(t1.C_CURRENT_CDEMO_SK,0) != coalesce(t2.C_CURRENT_CDEMO_SK,0)
    OR t1.C_FIRST_NAME!=t2.C_FIRST_NAME
    OR coalesce(t1.C_CURRENT_HDEMO_SK,0) != coalesce(t2.C_CURRENT_HDEMO_SK,0)
    OR t1.C_CURRENT_ADDR_SK!=t2.C_CURRENT_ADDR_SK
    OR t1.C_LAST_NAME!=t2.C_LAST_NAME
    OR t1.C_CUSTOMER_ID!=t2.C_CUSTOMER_ID
    OR coalesce(t1.C_LAST_REVIEW_DATE_SK,0) != coalesce(t2.C_LAST_REVIEW_DATE_SK,0)
    OR coalesce(t1.C_BIRTH_MONTH,0) != coalesce(t2.C_BIRTH_MONTH,0)
    OR t1.C_BIRTH_COUNTRY != t2.C_BIRTH_COUNTRY
    OR coalesce(t1.C_BIRTH_YEAR,0) != coalesce(t2.C_BIRTH_YEAR,0)
    OR coalesce(t1.C_BIRTH_DAY,0) != coalesce(t2.C_BIRTH_DAY,0)
    OR t1.C_EMAIL_ADDRESS != t2.C_EMAIL_ADDRESS
    OR coalesce(t1.C_FIRST_SHIPTO_DATE_SK,0) != coalesce(t2.C_FIRST_SHIPTO_DATE_SK,0)
    ) 
THEN UPDATE SET
    end_date = current_date();
    
    create or replace table SF_TPCDS.ANALYTICS.CUSTOMER_DIM as
        (select 
        C_SALUTATION,
        C_PREFERRED_CUST_FLAG,
        C_FIRST_SALES_DATE_SK,
        C_CUSTOMER_SK,
        C_LOGIN,
        C_CURRENT_CDEMO_SK,
        C_FIRST_NAME,
        C_CURRENT_HDEMO_SK,
        C_CURRENT_ADDR_SK,
        C_LAST_NAME,
        C_CUSTOMER_ID,
        C_LAST_REVIEW_DATE_SK,
        C_BIRTH_MONTH,
        C_BIRTH_COUNTRY,
        C_BIRTH_YEAR,
        C_BIRTH_DAY,
        C_EMAIL_ADDRESS,
        C_FIRST_SHIPTO_DATE_SK,
        CA_STREET_NAME,
        CA_SUITE_NUMBER,
        CA_STATE,
        CA_LOCATION_TYPE,
        CA_COUNTRY,
        CA_ADDRESS_ID,
        CA_COUNTY,
        CA_STREET_NUMBER,
        CA_ZIP,
        CA_CITY,
        CA_GMT_OFFSET,
        CD_DEP_EMPLOYED_COUNT,
        CD_DEP_COUNT,
        CD_CREDIT_RATING,
        CD_EDUCATION_STATUS,
        CD_PURCHASE_ESTIMATE,
        CD_MARITAL_STATUS,
        CD_DEP_COLLEGE_COUNT,
        CD_GENDER,
        HD_BUY_POTENTIAL,
        HD_DEP_COUNT,
        HD_VEHICLE_COUNT,
        HD_INCOME_BAND_SK,
        IB_LOWER_BOUND,
        IB_UPPER_BOUND,
        START_DATE,
        END_DATE
from SF_TPCDS.INTERMEDIATE.CUSTOMER_SNAPSHOT
LEFT JOIN TPCDS.RAW.customer_address ON c_current_addr_sk = ca_address_sk
LEFT join TPCDS.RAW.customer_demographics ON c_current_cdemo_sk = cd_demo_sk
LEFT join TPCDS.RAW.household_demographics ON c_current_hdemo_sk = hd_demo_sk
LEFT join TPCDS.RAW.income_band ON HD_INCOME_BAND_SK = IB_INCOME_BAND_SK
where end_date is null
        );
  END
  $$;


-- cron job task for customer dimensions
CREATE OR REPLACE TASK SF_TPCDS.ANALYTICS.creating_customer_dimension_using_scd_type_2
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = 'USING CRON * 8 * * * UTC'
    AS
CALL populating_customer_dimension_using_scd_type_2();

ALTER TASK SF_TPCDS.ANALYTICS.creating_customer_dimension_using_scd_type_2 RESUME;
EXECUTE TASK SF_TPCDS.ANALYTICS.creating_customer_dimension_using_scd_type_2;




-- DROP TASK SF_TPCDS.ANALYTICS.creating_customer_dimension_using_scd_type_2;
-- DROP PROCEDURE SF_TPCDS.ANALYTICS.populating_customer_dimension_using_scd_type_2();

--TRUNCATE table SF_TPCDS.INTERMEDIATE.CUSTOMER_SNAPSHOT;
--TRUNCATE table SF_TPCDS.ANALYTICS.CUSTOMER_DIM;