with
    stg_customers as (
        select
            customer_id
            , age
            , gender
            , email
            , '@' || split_part(email, '@', 2) as email_domain
        from {{ ref('stg_customers') }}
    )

    , stg_valid_domains as (
        select
            valid_domain
        from {{ ref('stg_valid_domains') }}
    )

    , joined as (
        select
            stg_customers.customer_id
            , stg_customers.age
            , stg_customers.gender
            , stg_customers.email
            , stg_valid_domains.valid_domain is not null as is_valid_email
        from stg_customers
        left join stg_haunted_house_tickets
            on stg_customers.email_domain = stg_valid_domains.valid_domain
    )

select *
from joined
