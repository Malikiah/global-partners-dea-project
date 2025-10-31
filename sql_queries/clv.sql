CREATE OR REPLACE VIEW CLV AS
WITH sum_order_options as (
    SELECT
        order_id,
        SUM(option_price * option_quantity) AS sum_options
    FROM public.order_item_options
    GROUP BY order_id
)
SELECT

    user_id,
    SUM(item_price * item_quantity) as total_items_value,
    SUM(soo.sum_options) AS total_options_value,
    SUM(item_price * item_quantity) + SUM(soo.sum_options) AS customer_lifetime_value

FROM public.order_items as oi
JOIN sum_order_options AS soo
    ON soo.order_id = oi.order_id
GROUP BY oi.user_id
ORDER BY customer_lifetime_value DESC
LIMIT 10;