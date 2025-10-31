{{
    config(
        materialized='table'
    )
}}

with latest_last_mile as(
    select 
        *,
        row_number() over(partition by no_sj order by pod_timestamp desc) as rnk_num
    from {{ ref("stg_deliveryproof") }}
    )

select 
    shipment_id,
    no_packinglist,
    no_sj,
    delivery_man,
    expedition,
    pod_date,
    pod_time,
    delivery_status,
    reason
from latest_last_mile
where rnk_num = 1