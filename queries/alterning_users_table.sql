-- see the column name and their data type
SELECT column_name,
    data_type
FROM information_schema.columns
WHERE table_name = 'dim_users';
-- change the data type of the column
ALTER TABLE dim_users
ALTER COLUMN first_name TYPE VARCHAR(255),
    ALTER COLUMN last_name TYPE VARCHAR(255),
    ALTER COLUMN user_uuid TYPE uuid USING user_uuid::uuid,
    ALTER COLUMN country_code TYPE VARCHAR(2);
-- sets user_uuid as the primary key
ALTER TABLE dim_users
ADD PRIMARY KEY (user_uuid);
-- create foreign key constraints that reference the primary keys of the users table
ALTER TABLE orders_table
ADD CONSTRAINT user_uuid_fk FOREIGN KEY (user_uuid) REFERENCES dim_users (user_uuid)