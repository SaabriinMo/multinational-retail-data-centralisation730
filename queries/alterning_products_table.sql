-- see the column name and their data type
SELECT column_name,
    data_type
FROM information_schema.columns
WHERE table_name = 'dim_products';
--- add a weight_class column
ALTER TABLE dim_products
ADD weight_class VARCHAR(255);
-- remove 'kg' from weight 
UPDATE dim_products
SET weight = REPLACE(weight, 'kg', '');
-- set a weight class for each weight
UPDATE dim_products
SET weight_class = CASE
        WHEN weight < 2 THEN 'Light'
        WHEN weight >= 2
        AND weight < 40 THEN 'Mid_Sized'
        WHEN weight >= 40
        AND weight < 140 THEN 'Heavy'
        ELSE 'Truck_Required'
    END;
-- rename the 'removed' column to 'still_available'
ALTER TABLE dim_products
    RENAME removed TO still_available;
-- set 'Still_avaliable' to true
UPDATE dim_products
SET still_available = TRUE
WHERE still_available = 'Still_avaliable';
-- set 'Removed' to false
UPDATE dim_products
SET still_available = FALSE
WHERE still_available = 'Removed';
-- Change the column types
ALTER TABLE dim_products
ALTER COLUMN product_price TYPE FLOAT USING product_price::FLOAT,
    ALTER COLUMN weight TYPE FLOAT USING weight::float,
    ALTER COLUMN "EAN" TYPE VARCHAR(255),
    ALTER COLUMN product_code TYPE VARCHAR(11),
    ALTER COLUMN uuid TYPE uuid USING uuid::uuid,
    ALTER COLUMN still_available TYPE BOOL USING still_available::boolean;
-- see if all the product_codes in the orders table can be found in the dim_products
SELECT DISTINCT product_code
FROM orders_table
WHERE product_code NOT IN (
        SELECT product_code
        FROM dim_products
    );
-- set product_code as the primary key
ALTER TABLE dim_products
ADD PRIMARY KEY (product_code);
-- create foreign key constraints that reference the primary key of the products table
ALTER TABLE orders_table
ADD CONSTRAINT product_code_fk FOREIGN KEY (product_code) REFERENCES dim_products (product_code);