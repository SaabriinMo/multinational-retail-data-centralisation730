-- see the column name and their data type
SELECT column_name,
    data_type
FROM information_schema.columns
WHERE table_name = 'dim_store_details';
-- change the data type of the column
ALTER TABLE dim_store_details
ALTER COLUMN longitude TYPE FLOAT USING longitude::FLOAT,
    ALTER COLUMN locality TYPE VARCHAR(255),
    ALTER COLUMN store_code TYPE VARCHAR(13),
    ALTER COLUMN staff_numbers TYPE SMALLINT USING staff_numbers::smallint,
    ALTER COLUMN store_type TYPE VARCHAR(255),
    ALTER COLUMN store_type DROP NOT NULL,
    ALTER COLUMN latitude TYPE FLOAT USING latitude::FLOAT,
    ALTER COLUMN country_code TYPE VARCHAR(3),
    ALTER COLUMN continent TYPE VARCHAR(255);
-- combine the 2 columns (lat and latitude) and store results in latitude column
UPDATE dim_store_details
SET latitude = COALESCE(CONCAT(latitude, lat), latitude);
-- drop the lat column
ALTER TABLE dim_store_details DROP COLUMN lat;
-- replace 'n/a' to null in latitude column
UPDATE dim_store_details
SET latitude = REPLACE(latitude, 'N/A', NULL);
-- replace 'n/a' to null in longitude column
UPDATE dim_store_details
SET longitude = REPLACE(longitude, 'N/A', NULL);
-- sets store_code as the primary key
ALTER TABLE dim_store_details
ADD PRIMARY KEY (store_code);
-- create foreign key constraints that reference the primary keys of the stores table
ALTER TABLE orders_table
ADD CONSTRAINT store_code_fk FOREIGN KEY (store_code) REFERENCES dim_store_details (store_code);
-- selects all the store_code that are in the orders table 
-- but not in the store table (for debugging purposes)
--  SELECT DISTINCT store_code
-- FROM orders_table
-- WHERE store_code NOT IN (
--         SELECT store_code
--         FROM dim_store_details
--     )