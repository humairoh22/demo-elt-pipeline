with stg_dim_destination as (
select 
    distinct
    destination as destination_name,
    case 
        when destination = 'Surabaya' then 'SBY'
        when destination = 'Bandung' then 'BDG'
        when destination = 'Serang' then 'SRG'
        when destination = 'Tangerang' then 'TGR'
        when destination = 'SO Yogyakarta' then 'SO-DIY'
        when destination = 'Tasikmalaya' then 'TSM'
        when destination = 'Kediri' then 'KDR'
        when destination = 'Sukabumi' then 'SKB'
        when destination = 'Bogor' then 'BGR'
        when destination = 'Lumajang' then 'LMJ'
        when destination = 'Lebak' then 'LBK'
        when destination = 'Semarang' then 'SMG'
        when destination = 'SO Sidoarjo' then 'SO-SDA'
        when destination = 'Malang' then 'MLG'
        when destination = 'Cianjur' then 'CJR'
        when destination = 'Solo' then 'SLO'
        when destination = 'Customer' then 'CUST'
        when destination = 'Kuningan' then 'KNG'
        when destination = 'Yogyakarta' then 'DIY'
        when destination = 'Cirebon' then 'CRB'
    end as code_destination

from {{ ref("stg_packinglist")}}
),

final_destination as (
    select
        {{dbt_utils.generate_surrogate_key(["destination_name"])}} as sk_destination_id,
        *,
        {{ dbt_date.now()}} as created_at,
        {{ dbt_date.now()}} as update_at
    
    from stg_dim_destination
)

select *
from final_destination