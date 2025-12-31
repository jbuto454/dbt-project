{{ config(
    materialized='view'
) }}

with
    source as (
        select
            haunted_house_id
            , house_name
            , park_area
            , theme
            , fear_level
            , house_size
            , current_timestamp() as loaded_at
            , row_number() over (
                partition by haunted_house_id
                order by loaded_at desc
            ) as row_num
        from {{ source('raw', 'raw_haunted_houses') }}
    )

select
    haunted_house_id,
    house_name,
    park_area,
    theme,
    fear_level,
    house_size,
    loaded_at
from source
where row_num = 1
