{{ config(
    materialized='view'
) }}

with
    source as (
        select
            ticket_id
            , customer_id
            , haunted_house_id
            , purchase_date
            , visit_date
            , ticket_type
            , ticket_price
            , current_timestamp() as loaded_at
            , row_number() over (
                partition by ticket_id
                order by loaded_at desc
            ) as row_num
        from {{ source('raw', 'raw_haunted_house_tickets') }}
    )

select
    ticket_id,
    customer_id,
    haunted_house_id,
    purchase_date,
    visit_date,
    ticket_type,
    ticket_price
    loaded_at
from source
where row_num = 1
