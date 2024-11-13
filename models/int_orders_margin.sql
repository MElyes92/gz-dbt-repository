WITH s AS (
    SELECT
        date_date,
        products_id,
        orders_id,
        COUNT(DISTINCT orders_id) AS total_orders,
        SUM(revenue) AS total_revenue,
        SUM(quantity) AS total_quantity
    FROM {{ ref('stg_raw__sales') }} 
    GROUP BY 
        orders_id,
        date_date, 
        products_id     
),
p AS (
    SELECT *
    FROM {{ ref('stg_raw__product') }} 
)
SELECT 
    s.date_date,
    s.orders_id,
    ROUND(SUM(s.total_quantity),2) AS quantity,
    ROUND(SUM(s.total_revenue),2) AS revenue,
    ROUND(SUM(p.purchase_price),2) AS purchase_price,
    ROUND(SUM(s.total_quantity * p.purchase_price),2) AS total_purchase_cost,
    ROUND(SUM(s.total_revenue - (s.total_quantity * p.purchase_price)),2) AS margin
FROM s
LEFT JOIN p ON s.products_id = p.products_id
GROUP BY 
s.orders_id,
s.date_date