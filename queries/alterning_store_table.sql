-- see the column name and their data type
SELECT column_name,
    data_type
FROM information_schema.columns
WHERE table_name = 'dim_store_details';
-- change the data type of the column
ALTER TABLE dim_store_details
ALTER COLUMN longitude TYPE FLOAT USING longitude::double precision,
    ALTER COLUMN locality TYPE VARCHAR(255),
    ALTER COLUMN store_code TYPE VARCHAR(13),
    ALTER COLUMN staff_numbers TYPE SMALLINT USING staff_numbers::smallint,
    ALTER COLUMN store_type TYPE VARCHAR(255),
    ALTER COLUMN store_type DROP NOT NULL,
    ALTER COLUMN latitude TYPE FLOAT USING latitude::double precision,
    ALTER COLUMN country_code TYPE VARCHAR(3),
    ALTER COLUMN continent TYPE VARCHAR(255);
-- sets store_code as the primary key
ALTER TABLE dim_store_details
ADD PRIMARY KEY (store_code);
-- create foreign key constraints that reference the primary keys of the stores table
ALTER TABLE orders_table
ADD CONSTRAINT user_uuid_fk FOREIGN KEY (store_code) REFERENCES dim_store_details (store_code)