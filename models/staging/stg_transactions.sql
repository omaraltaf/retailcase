with transactions as (
    select 
        timestamp_seconds(Timestamp) as order_datetime, 
        user_id as userid, 
        split(product_categories, ",")[safe_offset(0)] as prod1,
        split(product_categories, ",")[safe_offset(1)] as prod2,
        split(product_categories, ",")[safe_offset(2)] as prod3, 
        item1_revenue as product1_amount, 
        item2_revenue as product2_amount, 
        item3_revenue as product3_amount
     from `retail2022.OADB.transactions`
)
select * from transactions