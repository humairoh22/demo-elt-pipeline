{{
    config(
        materialized='table'
    )
}}

with stg_sales_order as (
    select * from {{ ref("stg_sales_order") }}
),

stg_delivery_order as (
    select * from {{ ref("stg_deliveryorder") }}
),

int_packinglist as (
    select * from {{ ref("int_latest_packinglist") }}

),

stg_transfer_status as (
    select * from {{ ref("stg_transferstatus") }}
),

int_delivery_proof as (
    select * from {{ ref("int_latest_delivery_proof") }}
),

dim_customers as (
    select * from {{ ref("dim_customers") }}
),

dim_destination as (
    select * from {{ ref("dim_destination") }}
),

dim_deliveryman as (
    select * from {{ ref("dim_deliveryman")}}
),

dim_date as (
    select * from {{ ref("dim_date") }}
),


promo as (
    SELECT
        idp.shipment_id,
        sso.no_pesan as no_so,
        sdo.no_faktur as no_sj,
        dic.sk_customer_id,
        ipl.no_packinglist,
        dde.sk_destination_id,
        ddm.sk_deliveryman_id,     
        dd1.date_id as so_date_id,
        dd2.date_id as sj_date_id,
        dd3.date_id as packinglist_date_id,
        dd4.date_id as first_mile_date_id,
        dd5.date_id as last_mile_date_id,
        case
            when sso.no_pesan is null then 0
            else 1
        end as is_so_created,
        case 
            when sdo.no_faktur is null then 0
            else 1
        end as is_sj_created,
        case
            when ipl.no_packinglist is null then 0
            else 1
        end as is_packinglist_created,

        case
            when sts.confirm_hub is null then 0
            else 1
        end as is_confirmed,

        case 
            when delivery_status is null then 0
            else 1
        end as is_pod

    from stg_sales_order sso
    left join stg_delivery_order sdo on sso.no_pesan = sdo.no_pesan
    left join int_packinglist ipl on sdo.no_faktur = ipl.no_sj
    left join stg_transfer_status sts on sts.no_packinglist = ipl.no_packinglist
    left join int_delivery_proof idp on idp.shipment_id = ipl.shipment_id
    left join dim_customers dic on sso.id_customer = dic.nk_customer_id
    left join dim_destination dde on dde.destination_name = ipl.destination
    left join dim_deliveryman ddm on ddm.deliveryman_name = ipl.delivery_man
    left join dim_date dd1 on dd1.date_actual = sso.tgl_pesan
    left join dim_date dd2 on dd2.date_actual = sdo.tgl_faktur
    left join dim_date dd3 on dd3.date_actual = ipl.packinglist_date
    left join dim_date dd4 on dd4.date_actual = sts.confirm_date
    left join dim_date dd5 on dd5.date_actual = idp.pod_date
)

select *
from promo