/* All the examples below are in relation to snowflake sql relational database */

create database ICONIC;

USE DATABASE ICONIC;

create or replace schema DWH;

CREATE FILE FORMAT DWH.LOAD_JSON 
    TYPE                   = 'JSON' 
    COMPRESSION            = 'AUTO' 
    ENABLE_OCTAL           = FALSE 
    ALLOW_DUPLICATE        = FALSE 
    STRIP_OUTER_ARRAY      = FALSE 
    STRIP_NULL_VALUES      = FALSE 
    IGNORE_UTF8_ERRORS     = FALSE
;

CREATE TABLE DWH.CUSTOMER_ORDERS_RAW_JSON 
(
    RAW_DATA VARIANT COMMENT 'Raw json data'
)
COMMENT = 'This table captures raw json data extracted from online shopping transactions.'
;

CREATE TABLE DWH.CUSTOMER_ORDERS
(
CUSTOMER_ID                 STRING   COMMENT 'ID of the customer - super duper hashed',
DAYS_SINCE_FIRST_ORDER      INTEGER  COMMENT 'Days since the first order was made',
DAYS_SINCE_LAST_ORDER       INTEGER  COMMENT 'Days since the last order was made',
IS_NEWSLETTER_SUBSCRIBER    STRING   COMMENT 'Flag for a newsletter subscriber',
ORDERS                      INTEGER  COMMENT 'Number of orders',
ITEMS                       INTEGER  COMMENT 'Number of items',
CANCELS                     INTEGER  COMMENT 'Number of cancellations - when the order is cancelled after being placed',
RETURNS                     INTEGER  COMMENT 'Number of returned orders',
DIFFERENT_ADDRESSES         INTEGER  COMMENT 'Number of times a different billing and shipping address was used',
SHIPPING_ADDRESSES          INTEGER  COMMENT 'Number of different shipping addresses used',
DEVICES                     INTEGER  COMMENT 'Number of unique devices used',
VOUCHERS                    INTEGER  COMMENT 'Number of times a voucher was applied',
CC_PAYMENTS                 INTEGER  COMMENT 'Number of times a credit card was used for payment',
PAYPAL_PAYMENTS             INTEGER  COMMENT 'Number of times PayPal was used for payment',
AFTERPAY_PAYMENTS           INTEGER  COMMENT 'Number of times AfterPay was used for payment',
APPLE_PAYMENTS              INTEGER  COMMENT 'Number of times Apple Pay was used for payment',
FEMALE_ITEMS                INTEGER  COMMENT 'Number of female items purchased',
MALE_ITEMS                  INTEGER  COMMENT 'Number of male items purchased',
UNISEX_ITEMS                INTEGER  COMMENT 'Number of unisex items purchased',
WAPP_ITEMS 					INTEGER  COMMENT 'Number of Women Apparel items purchased',
WFTW_ITEMS 					INTEGER  COMMENT 'Number of Women Footwear items purchased',
MAPP_ITEMS 					INTEGER  COMMENT 'Number of Men Apparel items purchased',
WACC_ITEMS 					INTEGER  COMMENT 'Number of Women Accessories items purchased',
MACC_ITEMS 					INTEGER  COMMENT 'Number of Men Accessories items purchased',
MFTW_ITEMS 					INTEGER  COMMENT 'Number of Men Footwear items purchased',
WSPT_ITEMS 					INTEGER  COMMENT 'Number of Women Sport items purchased',
MSPT_ITEMS 					INTEGER  COMMENT 'Number of Men Sport items purchased',
CURVY_ITEMS                 INTEGER  COMMENT 'Number of Curvy items purchased',
SACC_ITEMS                  INTEGER  COMMENT 'Number of Sport Accessories items purchased',
MSITE_ORDERS                INTEGER  COMMENT 'Number of Mobile Site orders',
DESKTOP_ORDERS              INTEGER  COMMENT 'Number of Desktop orders',
ANDROID_ORDERS              INTEGER  COMMENT 'Number of Android app orders',
IOS_ORDERS                  INTEGER  COMMENT 'Number of iOS app orders',
OTHER_DEVICE_ORDERS         INTEGER  COMMENT 'Number of Other device orders',
WORK_ORDERS                 INTEGER  COMMENT 'Number of orders shipped to work',
HOME_ORDERS                 INTEGER  COMMENT 'Number of orders shipped to home',
PARCELPOINT_ORDERS          INTEGER  COMMENT 'Number of orders shipped to a parcelpoint',
OTHER_COLLECTION_ORDERS     INTEGER  COMMENT 'Number of orders shipped to other collection points',
AVERAGE_DISCOUNT_ONOFFER    FLOAT   COMMENT 'Average discount rate of items typically purchased',
AVERAGE_DISCOUNT_USED       FLOAT   COMMENT 'Average discount finally used on top of existing discount',
REVENUE                     FLOAT   COMMENT '$ Dollar spent overall per person'
)
COMMENT = 'This table captures data extracted from raw json table which holds customer order information.'
;

INSERT INTO DWH.CUSTOMER_ORDERS
(
	CUSTOMER_ID
, 	DAYS_SINCE_FIRST_ORDER
, 	DAYS_SINCE_LAST_ORDER
, 	IS_NEWSLETTER_SUBSCRIBER
, 	ORDERS
, 	ITEMS
, 	CANCELS
, 	RETURNS
, 	DIFFERENT_ADDRESSES
, 	SHIPPING_ADDRESSES
, 	DEVICES
, 	VOUCHERS
, 	CC_PAYMENTS
, 	PAYPAL_PAYMENTS
, 	AFTERPAY_PAYMENTS
, 	APPLE_PAYMENTS
, 	FEMALE_ITEMS
, 	MALE_ITEMS
, 	UNISEX_ITEMS
, 	WAPP_ITEMS
, 	WFTW_ITEMS
, 	MAPP_ITEMS
, 	WACC_ITEMS
, 	MACC_ITEMS
, 	MFTW_ITEMS
, 	WSPT_ITEMS
, 	MSPT_ITEMS
, 	CURVY_ITEMS
, 	SACC_ITEMS
, 	MSITE_ORDERS
, 	DESKTOP_ORDERS
, 	ANDROID_ORDERS
, 	IOS_ORDERS
, 	OTHER_DEVICE_ORDERS
, 	WORK_ORDERS
, 	HOME_ORDERS
, 	PARCELPOINT_ORDERS
, 	OTHER_COLLECTION_ORDERS
, 	AVERAGE_DISCOUNT_ONOFFER
, 	AVERAGE_DISCOUNT_USED
, 	REVENUE
)
SELECT
	raw_Data:customer_id::string
, 	CASE WHEN raw_Data:days_since_first_order::number > raw_Data:days_since_last_order::number THEN raw_Data:days_since_first_order::number ELSE raw_Data:days_since_last_order::number END 
, 	CASE WHEN raw_Data:days_since_last_order::number < raw_Data:days_since_first_order::number THEN raw_Data:days_since_last_order::number ELSE raw_Data:days_since_first_order::number END 
, 	raw_Data:is_newsletter_subscriber::String
, 	raw_Data:orders::number
, 	raw_Data:items::number
, 	raw_Data:cancels::number
, 	raw_Data:returns::number
, 	raw_Data:different_addresses::number
, 	raw_Data:shipping_addresses::number
, 	raw_Data:devices::number
, 	raw_Data:vouchers::number
, 	raw_Data:cc_payments::number
, 	raw_Data:paypal_payments::number
, 	raw_Data:afterpay_payments::number
, 	raw_Data:apple_payments::number
, 	raw_Data:female_items::number
, 	raw_Data:male_items::number
, 	raw_Data:unisex_items::number
, 	raw_Data:wapp_items::number
, 	raw_Data:wftw_items::number
, 	raw_Data:mapp_items::number
, 	raw_Data:wacc_items::number
, 	raw_Data:macc_items::number
, 	raw_Data:mftw_items::number
, 	raw_Data:wspt_items::number
, 	raw_Data:mspt_items::number
, 	raw_Data:curvy_items::number
, 	raw_Data:sacc_items::number
, 	raw_Data:msite_orders::number
, 	raw_Data:desktop_orders::number
, 	raw_Data:android_orders::number
, 	raw_Data:ios_orders::number
, 	raw_Data:other_device_orders::number
, 	raw_Data:work_orders::number
, 	raw_Data:home_orders::number
, 	raw_Data:parcelpoint_orders::number
, 	raw_Data:other_collection_orders::number
, 	raw_Data:average_discount_onoffer::float
, 	raw_Data:average_discount_used::float
, 	raw_Data:revenue::float
from DWH.CUSTOMER_ORDERS
;


/* What was the total revenue to the nearest dollar for customers who have paid by credit card? */

SELECT CEIL(SUM(REVENUE)) Total_Revenue-- 50372282
FROM DWH.CUSTOMER_ORDERS
WHERE cc_payments = 1
;

/* What percentage of customers who have purchased female items have paid by credit card?*/

SELECT  COUNT_if(CC_PAYMENTS=1 AND FEMALE_ITEMS>0) / COUNT(*) * 100 Percentage_Of_Customers_Paid_By_Credid_Card
FROM DWH.CUSTOMER_ORDERS
;

/* What was the average revenue for customers who used either iOS, Android or Desktop? */

SELECT AVG(REVENUE) Average_Revenue
FROM DWH.CUSTOMER_ORDERS 
WHERE  (IOS_ORDERS > 0 OR ANDROID_ORDERS > 0 OR DESKTOP_ORDERS > 0) 
;

/* We want to run an email campaign promoting a new mens luxury brand. Can you provide a list of customers we should send to? */

SELECT CUSTOMER_ID 
FROM DWH.LOAD_TEST_DATA 
WHERE (DAYS_SINCE_FIRST_ORDER - DAYS_SINCE_LAST_ORDER) > 180
AND IS_NEWSLETTER_SUBSCRIBER = 'Y'
AND "RETURNS" = 0
AND CANCELS = 0
AND (MALE_ITEMS > 0 OR UNISEX_ITEMS > 0)
AND MACC_ITEMS > 0 
AND (MSPT_ITEMS > 0 OR SACC_ITEMS > 0)
;



/* Productionisation 

Strategy without ETL tool 

- Create a task in Snowflake using create task statement 
- Task can be scheduled to run on daily basis
- Inside task we can have stored procedure to run these statements 
- Stored procedure will be running above statements and unloading data into another table or any other file format depending on the business requirements.

Strategy ETL tool 

- For an example Matillion ETL tool can be used to connect to snowflake 
- Create config tables for job, job audit, run status and sequence
- Add all the sql queries into different sql component 
- Job sequence starts running for current batch 
- Once job starts running it will log entry into job audit table and update run status against it
- Either publish output into another table or into any other file format 
- Update job run status table once job finishes 
- End the current sequence

*/
