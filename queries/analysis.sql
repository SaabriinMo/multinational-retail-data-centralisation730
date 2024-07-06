/*
 Task 1: How many stores does the bussiness have and in which country?
 */
SELECT country_code,
    COUNT(store_code) AS total_no_stores
FROM dim_store_details
GROUP BY country_code
ORDER BY total_no_stores DESC;
/*
 Task 2: which location currently have the most sales?
 */
SELECT locality,
    COUNT(*) AS total_no_stores
FROM dim_store_details
GROUP BY locality
ORDER BY total_no_stores DESC;
/*
 Task 3: which month produced the largest amount of sales
 */
SELECT SUM(product_price * product_quantity) AS total_sales,
    month
FROM dim_products as products
    LEFT JOIN orders_table as orders ON products.product_code = orders.product_code
    LEFT JOIN dim_date_times ON orders.date_uuid = dim_date_times.date_uuid
GROUP BY month
ORDER BY total_sales DESC;
/*
 Task 4: how many sales are coming from online?
 */
SELECT COUNT(product_quantity) as product_quantity_count,
    SUM(product_quantity),
    CASE
        WHEN store_type = 'Web Portal' THEN 'Web'
        ELSE 'Offline'
    END AS location
FROM dim_products as products
    LEFT JOIN orders_table as orders ON products.product_code = orders.product_code
    LEFT JOIN dim_store_details as stores ON orders.store_code = stores.store_code
GROUP BY location;
/*
 Task 5: What percentage of sales comes through each type of store
 */
SELECT store_type,
    round(SUM(product_quantity * product_price)::numeric, 2) as sales,
    round(SUM(product_quantity * product_price)::numeric, 2) / SUM(SUM(product_quantity * product_price)) OVER () as percentage
FROM dim_store_details as stores
    LEFT JOIN orders_table as orders ON stores.store_code = orders.store_code
    LEFT JOIN dim_products as products on orders.product_code = products.product_code
GROUP BY store_type;
/*
 Task 6: Which month in each year produced the highest cost in sales?
 */
SELECT SUM(product_price * product_quantity) AS total_sales,
    year,
    month
FROM dim_products as products
    LEFT JOIN orders_table as orders ON products.product_code = orders.product_code
    LEFT JOIN dim_date_times as times ON orders.date_uuid = times.date_uuid
GROUP BY year,
    month
ORDER BY total_sales DESC;
/*
 Task 7: What is our staff headcount?
 */
SELECT SUM(staff_numbers) AS total_staff_numbers,
    country_code
FROM dim_store_details
GROUP BY country_code;
/*
 Task 8: Which German Store Type is selling the most?
 */
SELECT store_type,
    round(SUM(product_quantity * product_price)::numeric, 2) as total_sales,
    country_code
FROM dim_store_details as stores
    LEFT JOIN orders_table as orders ON stores.store_code = orders.store_code
    LEFT JOIN dim_products as products on orders.product_code = products.product_code
WHERE country_code = 'DE'
GROUP BY store_type,
    country_code
ORDER BY total_sales;
/*
 Task 9: How quickly is the company making sales
 */
WITH purchase_time AS (
    SELECT product_code,
        year,
        month,
        day,
        TO_TIMESTAMP(
            CONCAT(year, '-', month, '-', day, ' ', timestamp),
            'YYYY-MM-DD HH24:MI:SS.FF'
        ) AS time_stamp
    FROM orders_table as orders
        LEFT JOIN dim_date_times as dates ON orders.date_uuid = dates.date_uuid
),
--- Calculate the next purchase time for each order
calculate_next_purchase AS (
    SELECT product_code,
        year,
        day,
        month,
        time_stamp,
        LEAD(time_stamp) OVER (
            ORDER BY time_stamp
        ) as next_time_sold
    FROM purchase_time
),
-- convert the time of each order in seconds	
convert_to_epoch AS (
    SELECT year,
        EXTRACT(
            EPOCH
            FROM(time_stamp::timestamp without time zone)
        ) AS time_stamp_seconds,
        EXTRACT(
            EPOCH
            FROM(next_time_sold::timestamp without time zone)
        ) AS next_time_stamp_seconds
    FROM calculate_next_purchase
),
-- calculate the difference between orders by year
avg_time_taken_per_year AS (
    SELECT year,
        AVG(next_time_stamp_seconds - time_stamp_seconds) as avg_time
    FROM convert_to_epoch
    GROUP BY year
    ORDER BY year
) -- structure the results in desired format
SELECT year,
    CONCAT(
        'hours:',
        ' ',
        ROUND(avg_time / 3600),
        ' ',
        'minutes:',
        ' ',
        FLOOR((avg_time % 3600) / 60),
        ' ',
        'seconds:',
        ' ',
        ROUND(avg_time % 60),
        ' ',
        'milliseconds:',
        ' ',
        ROUND(((avg_time % 60) - FLOOR(avg_time % 60)) * 1000)
    ) AS actual_time_taken
FROM avg_time_taken_per_year
ORDER BY avg_time DESC;