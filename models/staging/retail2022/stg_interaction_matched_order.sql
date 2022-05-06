
with traffic as (
    select * from {{ ref('stg_traffic')}}
), 
transactions as (
    select * from {{ ref('stg_transactions')}}
),

interaction_matched_order as (
    select 
        a.*, 
        b.interaction_datetime, 
        b.channel, 
        b.campaign_type, 
        b.campaign 
    from transactions a left outer join traffic b 
    on a.userid = b.userid
    where
    b.interaction_datetime = 
        (select 
            max(interaction_datetime)
        from traffic c
        where 
            c.userid = a.userid and 
            c.interaction_datetime<a.order_datetime and 
            (
                c.campaign = a.prod1 or
                c.campaign = a.prod2 or
                c.campaign = a.prod3 or
                c.campaign = 'General' or
                c.campaign is null
            )
        )
)

select * from interaction_matched_order