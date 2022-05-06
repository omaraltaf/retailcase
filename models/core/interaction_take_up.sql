{{
  config(
    materialized='table'
  )
}}

with interactions as (
    select * from {{ ref('stg_traffic') }}
), 

latest_interaction_with_order as (
    select * from {{ ref('stg_latest_interaction_to_any_order_type') }}
), 

interaction_take_up as 
(
    select 
        a.*, 
        b.prod1, 
        b.prod2, 
        b.prod3, 
        b.product1_amount,    
        b.product2_amount,
        b.product3_amount
    from interactions a left outer join latest_interaction_with_order b on 
        concat(a.userid, a.interaction_datetime)=concat(b.userid, b.interaction_datetime)
    
)
select * from interaction_take_up