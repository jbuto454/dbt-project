{{ config(
    materialized='view'
) }}

with
    source as (
        select
            customer_id
            , age
            , gender
            , email
            , current_timestamp() as loaded_at
            , row_number() over (
                partition by customer_id
                order by loaded_at desc
            ) as row_num
        from {{ source('raw', 'raw_customers') }}
    )

select
    customer_id,
    age,
    gender,
    email,
    loaded_at
from source
where row_num = 1
