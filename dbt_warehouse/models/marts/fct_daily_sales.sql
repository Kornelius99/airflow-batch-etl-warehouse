-- Mart model: daily sales aggregated by region and product category.
-- This is the table BI tools / analysts would query directly.

with orders as (

    select * from {{ ref('stg_orders') }}

)

select
    order_date,
    region,
    product_category,
    count(order_id)              as order_count,
    count(distinct customer_id)  as distinct_customers,
    sum(quantity)                as units_sold,
    sum(total_amount)            as revenue
from orders
group by order_date, region, product_category
order by order_date, region, product_category
