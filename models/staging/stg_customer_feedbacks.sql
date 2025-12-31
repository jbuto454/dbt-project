{{ config(
    materialized='view'
) }}

with
    source as (
        select
            feedback_id
            , ticket_id
            , rating
            , comments
            , current_timestamp() as loaded_at
            , row_number() over (
                partition by feedback_id
                order by loaded_at desc
            ) as row_num
        from {{ source('raw', 'raw_customer_feedbacks') }}
    )

select
    feedback_id,
    ticket_id,
    rating,
    comments,
    loaded_at
from source
where row_num = 1
