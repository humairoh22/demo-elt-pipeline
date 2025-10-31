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

int_latest_packinglist as (
    select * from {{ ref("int_latest_packinglist") }}
),

stg_transfer_status as (
    select * from {{ ref("stg_transferstatus") }}
),

int_latest_delivery_proof as (
    select * from {{ ref("int_latest_delivery_proof") }}
),

dim_customers as (
    select * from {{ ref("dim_customers") }}
),

dim_delivery_man as (
    select * from {{ ref("dim_deliveryman")}}
),

dim_origin as (
    select * from {{ ref("dim_origin")}}
),

dim_destination as (
    select * from {{ ref("dim_destination") }}
),
dim_expedition as (
    select * from {{ ref("dim_expedition")}}
),

dim_leadtime as (
    select * from {{ ref("dim_leadtime") }}
),

dim_date as (
    select * from {{ref("dim_date")}}
),

dim_time as (
    select * from {{ref("dim_time")}}
),

fact_delivery as (
    select
        pl.shipment_id,
        so.no_po,
        so.no_pesan as no_so,
        ddo.no_faktur as no_sj,
        dc.sk_customer_id as sk_customer_id,
        so.total_harga as value_order,
        pl.no_packinglist,
        ori.sk_origin_id as sk_origin_id,
        des.sk_destination_id as sk_destination_id,
        ts.confirm_hub,
        ddm.sk_deliveryman_id,
        case
            when dex2.sk_expedition_id is null or dex2.sk_expedition_id = '' then dex1.sk_expedition_id
            else dex2.sk_expedition_id
        end as sk_expedition_id,
        dd6.date_id as po_expired_id,
        dd1.date_id as so_date_id,
        dd2.date_id as sj_date_id,
        dd3.date_id as packinglist_date_id,
        dt1.time_id as packinglist_time_id,
        dd4.date_id as first_mile_date_id,
        dt2.time_id as first_mile_time_id,
        dd5.date_id as last_mile_date_id,
        dt3.time_id as last_mile_time_id,
        dl.area_id as target_leadtime_id,
        dp.delivery_status as delivery_status,
        dp.reason as reason_undelivery

    from stg_sales_order as so
    join stg_delivery_order as ddo on so.no_pesan = ddo.no_pesan
    join dim_customers as dc on so.id_customer = dc.nk_customer_id
    join int_latest_packinglist as pl on pl.no_sj = ddo.no_faktur
    join stg_transfer_status as ts on pl.no_packinglist = ts.no_packinglist
    join dim_origin as ori on ori.origin = pl.origin
    join dim_destination as des on des.destination_name = pl.destination
    join dim_date as dd1 on dd1.date_actual = so.tgl_pesan
    join dim_date as dd2 on dd2.date_actual = ddo.tgl_faktur
    join dim_date as dd3 on dd3.date_actual = pl.packinglist_date
    join dim_time as dt1 on dt1.time_actual = pl.packinglist_time
    left join int_latest_delivery_proof as dp on pl.shipment_id = dp.shipment_id
    left join dim_expedition as dex1 on dex1.expedition_name = pl.expedition
    left join dim_expedition as dex2 on dex2.expedition_name = dp.expedition
    left join dim_delivery_man as ddm on ddm.deliveryman_name = pl.delivery_man
    left join dim_leadtime as dl on dl.kota_pelanggan = dc.city
    left join dim_date as dd4 on dd4.date_actual = ts.confirm_date
    left join dim_date as dd5 on dd5.date_actual = dp.pod_date
    left join dim_date as dd6 on dd6.date_actual = so.po_expired
    left join dim_time as dt2 on dt2.time_actual = ts.confirm_time
    left join dim_time as dt3 on dt3.time_actual = dp.pod_time


),

delivery_attempt as (
    select
        distinct no_sj,
        count(*) as count_delivery_attempt
    from {{ ref("stg_deliveryproof")}}
    group by 1
)
,

final_fct_delivery as (
    select 
    fd.*,
    dat.count_delivery_attempt
    from fact_delivery as fd
    left join delivery_attempt as dat on fd.no_sj = dat.no_sj
 
    )

select * from final_fct_delivery


