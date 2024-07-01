-- see the column name and their data type
SELECT column_name,
    data_type
FROM information_schema.columns
WHERE table_name = 'dim_card_details';
-- change the data type of the column
ALTER TABLE dim_card_details
ALTER COLUMN card_number TYPE VARCHAR(19),
    ALTER COLUMN expiry_date TYPE VARCHAR(5),
    ALTER COLUMN card_provider TYPE VARCHAR(255);
-- set card_number as the primary key
ALTER TABLE dim_card_details
ADD PRIMARY KEY (card_number);
-- create foreign key constraints that reference the primary keys of the card details table
ALTER TABLE orders_table
ADD CONSTRAINT card_number_fk FOREIGN KEY (card_number) REFERENCES dim_card_details (card_number);
-- selects all the card numbers that are in the orders table 
-- but not in the card details table (for debugging purposes)
SELECT DISTINCT card_number
FROM orders_table
WHERE card_number NOT IN (
        SELECT card_number
        FROM dim_card_details
    )