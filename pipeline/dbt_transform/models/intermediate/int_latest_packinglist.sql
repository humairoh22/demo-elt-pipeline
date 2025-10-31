{{
    config(
        materialized='table'
    )
}}

with latest_packinglist as (
        select 
            *,
            row_number() over(partition by no_sj order by packinglist_created_at desc) as rnk_num
        from {{ ref("stg_packinglist") }}
    )

select
    shipment_id,
    no_packinglist,
    no_sj,
    code as code_packinglist,
    packinglist_date,
    packinglist_time,
    origin,
    destination,
    delivery_man,
    expedition,
    awb,
    expedition_fee,
    total_pl,
    notes

from latest_packinglist
where rnk_num = 1
