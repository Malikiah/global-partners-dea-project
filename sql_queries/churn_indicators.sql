CREATE OR REPLACE VIEW CHURN_INDICATORS AS
WITH order_gap AS
(
    SELECT
        user_id,
        CREATION_TIME_UTC as order_date,
        LAG(oi.CREATION_TIME_UTC) OVER (
            PARTITION BY user_id
            ORDER BY order_date
        ) as previous_order_date
    FROM public.order_items AS oi
),
sum_order_options as (
    SELECT
        order_id,
        SUM(option_price * option_quantity) AS sum_options
    FROM public.order_item_options
    GROUP BY order_id
),
sum_orders as (
    SELECT
        EXTRACT(year FROM so.CREATION_TIME_UTC) AS order_year,
        user_id,
        SUM(item_price * item_quantity) + SUM(soo.sum_options) AS total_value

    FROM public.order_items AS so
    JOIN sum_order_options as soo
        ON soo.order_id = so.order_id
    GROUP BY user_id, order_year

),
yoy_change AS (
    SELECT
        so.user_id,
        so.order_year,
        so.total_value AS current_year_total_value,
        LAG(so.total_value, 1) OVER (
            PARTITION BY user_id
            ORDER BY order_year
        ) AS previous_year_total_value
    FROM sum_orders AS so
)
SELECT
    oi.user_id,
    MAX(oi.CREATION_TIME_UTC) as last_order_date,
    DATEDIFF(day, MAX(oi.CREATION_TIME_UTC), CURRENT_DATE) AS days_since_last_order,
    CASE
        WHEN DATEDIFF(day, MAX(oi.CREATION_TIME_UTC), CURRENT_DATE) > 45 THEN 'at risk'
        WHEN DATEDIFF(day, MAX(oi.CREATION_TIME_UTC), CURRENT_DATE) > 45 THEN ''
        END AS churn_status,
    COALESCE(AVG(DATEDIFF(day, og.previous_order_date, og.order_date)), 0) AS average_between_orders,
    (yc.current_year_total_value - yc.previous_year_total_value)
        / NULLIF(yc.previous_year_total_value, 0) * 100.0 AS yoy_percentage_change
FROM public.order_items AS oi
JOIN order_gap AS og
    ON og.user_id = oi.user_id
JOIN yoy_change AS yc
    ON yc.user_id = og.user_id
GROUP BY oi.user_id, yc.current_year_total_value, yc.previous_year_total_value
ORDER BY days_since_last_order
LIMIT 10;
