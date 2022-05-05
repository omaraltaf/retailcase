with traffic as (
    select 
        timestamp_seconds(Timestamp) as interaction_datetime, 
        user_id as userid, 
        source as channel, 
        campaign_type, 
        campaign
     from `retail2022.OADB.traffic`
)
select * from traffic