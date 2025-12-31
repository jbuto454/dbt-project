{{ config(
    materialized='view'
) }}

with
    source as (
        select
            valid_domain
        from {{ source('raw', 'valid_domains') }}
    )

select
    valid_domain,
from source