-- see the column name and their data type
SELECT column_name,
    data_type
FROM information_schema.columns
WHERE table_name = 'dim_date_times';
-- change the data type of the column
ALTER TABLE dim_date_times
ALTER COLUMN month TYPE VARCHAR(2),
    ALTER COLUMN year TYPE VARCHAR(4),
    ALTER COLUMN day TYPE VARCHAR(2),
    ALTER COLUMN time_period TYPE VARCHAR(10),
    ALTER COLUMN date_uuid TYPE uuid USING date_uuid::uuid;
-- set date_uuid as the primary key
ALTER TABLE dim_date_times
ADD PRIMARY KEY (date_uuid);
-- create foreign key constraints that reference the primary keys of the card details table
ALTER TABLE orders_table
ADD CONSTRAINT date_uuid_fk FOREIGN KEY (date_uuid) REFERENCES dim_date_times (date_uuid)