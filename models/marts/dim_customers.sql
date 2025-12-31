with
    stg_orders as (
        select
            id
            , customer
            , store_id
            , order_total
        from {{ ref('stg_orders') }}
    )

    , stg_customers as (
        select
            id
            , name
        from {{ ref('stg_customers') }}
    )

    , stores_agg as (
        select
            customer
            , listagg((store_id), ', ') as stores_purchased_from
            , sum(order_total) as total_amount
        from stg_orders
        group by customer
    )

    , joined as (
        select
            stores_agg.customer
            , stores_agg.stores_purchased_from
            , stores_agg.total_amount
            , stg_customers.name
            , stg_customers.id
        from stg_customers
        left join stores_agg
            on stg_customers.id = stores_agg.customer
    )

select *
from joined
