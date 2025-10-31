with stg_dim_origin as (
select
	distinct 
	 case 
	 	when origin = 'Sales Office DIY' then 'Branch DIY'
	 	else origin
	 end as origin
from {{ref("stg_packinglist")}}
),

final_dim_origin as (
	select
		{{dbt_utils.generate_surrogate_key(["origin"])}} as sk_origin_id,
		origin,
		{{ dbt_date.now()}} as created_at,
		{{ dbt_date.now()}} as update_at
	from stg_dim_origin
)

select * from final_dim_origin