WITH s AS (
    SELECT
        date_date,
        products_id,
        COUNT(DISTINCT orders_id) AS total_orders,
        SUM(revenue) AS total_revenue,
        SUM(quantity) AS total_quantity
    FROM {{ ref('stg_raw__sales') }} 
    GROUP BY 
        date_date, 
        products_id
),

p AS (
    SELECT *
    FROM {{ ref('stg_raw__product') }} 
)

SELECT 
    s.date_date,
    s.products_id,
    s.total_quantity,
    s.total_revenue,
    p.purchase_price,
    s.total_quantity * p.purchase_price AS total_purchase_cost,
    s.total_revenue - (s.total_quantity * p.purchase_price) AS margin
FROM s
LEFT JOIN p ON s.products_id = p.products_id
