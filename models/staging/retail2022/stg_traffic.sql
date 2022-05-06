with traffic as (
    select 
        timestamp_seconds(Timestamp) as interaction_datetime, 
        user_id as userid, 
        source as channel, 
        campaign_type, 
        case
            when campaign = 'Electro' then 'Electronics'
            when campaign = 'Pharma' then 'Pharmacy'
            else campaign
        end as campaign
    from {{ source('OADB', 'traffic') }}
)
select * from traffic