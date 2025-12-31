with
    stg_haunted_houses as (
        select
            haunted_house_id
            , house_name
            , park_area
            , theme
            , fear_level
            , house_size as house_size_in_ft2
            , house_size * 0.0929 as house_size_m2
        from {{ ref('stg_haunted_houses') }}
    )

select *
from stg_haunted_houses
