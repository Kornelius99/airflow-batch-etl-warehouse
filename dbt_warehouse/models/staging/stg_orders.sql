-- Staging model: light cleaning/typing of the raw orders table loaded by Airflow.
-- Staging models should do minimal logic - renaming, casting, basic filtering -
-- so downstream marts can trust a consistent, well-typed contract.

with source as (

    select * from {{ source('raw', 'orders') }}

),

renamed as (

    select
        order_id,
        customer_id,
        order_date::date as order_date,
        region,
        product_category,
        quantity::int as quantity,
        unit_price::numeric(10, 2) as unit_price,
        total_amount::numeric(10, 2) as total_amount
    from source
    where order_id is not null
      and customer_id is not null
      and total_amount > 0

)

select * from renamed
