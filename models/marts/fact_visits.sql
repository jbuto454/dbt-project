with
    stg_customer_feedbacks as (
        select
            ticket_id
            , comments
            , rating
        from {{ ref('stg_customer_feedbacks') }}
    )

    , stg_haunted_house_tickets as (
        select
            ticket_id
            , customer_id
            , haunted_house_id
            , purchase_date
            , visit_date
            , ticket_type
            , ticket_price
        from {{ ref('stg_haunted_house_tickets') }}
    )

    , joined as (
        select
            stg_haunted_house_tickets.ticket_id
            , stg_haunted_house_tickets.customer_id
            , stg_haunted_house_tickets.haunted_house_id
            , stg_haunted_house_tickets.purchase_date
            , stg_haunted_house_tickets.visit_date
            , stg_haunted_house_tickets.ticket_type
            , stg_haunted_house_tickets.ticket_price
            , stg_customer_feedbacks.comments
            , stg_customer_feedbacks.rating
        from stg_haunted_house_tickets
        left join stg_customer_feedbacks
            on stg_haunted_house_tickets.ticket_id = stg_customer_feedbacks.ticket_id
    )

select *
from joined
