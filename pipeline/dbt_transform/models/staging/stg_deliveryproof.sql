with dp as (

select 
    no_id as nk_proof_id,
    code,
    {{ clean_whitespace("split_part(no_sj, '_', 1)") }} as no_sj,
    {{ clean_whitespace("split_part(no_sj, '_', 3)") }} as no_packinglist,
    split_part(no_sj, '_', 2) as customer_name,
    case 
        when delivery_status = 'rud' then 'Return Upon Delivery'
        else delivery_status
    end as delivery_status,
    reason,
    delivery_man,
    license_plate,
    case
        when expedition ilike '%smr%' then 'SMR'
        when expedition = 'Sm' then 'SMR'
        when expedition ilike 'adex' then 'ADEX'
    end as expedition,
    pod_date as pod_timestamp,
    pod_date::date as pod_date,
    TO_CHAR(pod_date, 'HH24:MI:00') as pod_time,
    notes,
    pod_image as pod_link_image

from {{ source("public","deliveryproof")}}
WHERE is_deleted = False
)

select
    {{ dbt_utils.generate_surrogate_key(["no_packinglist", "no_sj"]) }} as shipment_id,
    dp.*
from dp