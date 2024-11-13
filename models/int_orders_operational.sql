SELECT*,
/*sm.orders_id,
sm.date_date,*/
ROUND(sm.margin+sh.shipping_fee-sh.logcost-sh.ship_cost,2) AS operational_margin
FROM {{ ref('int_orders_margin') }} AS sm
LEFT JOIN {{ ref('stg_raw__ship') }} sh
USING(orders_id)

