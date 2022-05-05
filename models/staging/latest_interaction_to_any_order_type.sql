with traffic as (
    select * from {{ ref('stg_traffic')}}
), 
transactions as (
    select * from {{ ref('stg_transactions')}}
),

latest_interaction_to_any_order_type as (
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
            c.interaction_datetime<a.order_datetime
        )

        
)
select * from latest_interaction_to_any_order_type order by userid