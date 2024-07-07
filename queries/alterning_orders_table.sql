-- see the column name and their data type
SELECT column_name,
    data_type
FROM information_schema.columns
WHERE table_name = 'orders_table';
-- change the data type of the column of the orders table 
ALTER TABLE orders_table
ALTER COLUMN date_uuid TYPE uuid USING date_uuid::uuid,
    ALTER COLUMN user_uuid TYPE uuid USING user_uuid::uuid,
    ALTER COLUMN card_number TYPE VARCHAR(19),
    ALTER COLUMN store_code TYPE VARCHAR(12),
    ALTER COLUMN product_code TYPE VARCHAR(11),
    ALTER COLUMN product_quantity TYPE float using product_quantity::float;