select
    {{ dbt_utils.generate_surrogate_key(['expedition_id '])}} as sk_expedition_id,
    expedition_id as nk_expedition_id,
    case 
        when expedition_name like '%ADEX%' then 'ADEX'
        else expedition_name
    end as expedition_name,
    {{ dbt_date.now() }} as created_at,
    {{ dbt_date.now() }} as update_at

from {{ ref("stg_expedition")}}