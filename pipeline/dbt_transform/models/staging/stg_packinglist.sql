{% set wrong_notes = ['BATAL', 'SALAH'] %}

with cleaning_pl AS (
	SELECT  
		packinglist_id,
		no_packinglist,
		regexp_split_to_table(sj_numbers, ',') AS sj_numbers,
		code,
		packinglist_time as packinglist_created_at,
		date(packinglist_time) as packinglist_date,
		TO_CHAR(packinglist_time, 'HH24:MI:00') as packinglist_time,
		CASE 
			WHEN origin = 'Sales Office DIY' THEN 'Branch DIY'
			ELSE origin
		END AS origin
		,
		CASE 
			WHEN destination = 'Pelanggan' THEN 'Customer'
			ELSE destination
		END AS destination
		,
		delivery_man,
		CASE 
			WHEN expedition LIKE '%ADEX%' THEN 'ADEX'
			ELSE expedition
		END AS expedition,
		awb,
		expedition_fee,
		total_pl,
		coalesce(notes, '') as notes
	FROM {{ source("public", "packinglist")}}
	WHERE is_deleted = False
		),
		
fnl_cleaning_pl AS (
	SELECT	
		packinglist_id AS nk_packinglist,
		{{ clean_whitespace("no_packinglist") }} as no_packinglist,
		{{ clean_whitespace("split_part(sj_numbers, '_', 1)") }} as no_sj,
		split_part(sj_numbers, '_', 2) AS customer_name,
		code,
		packinglist_created_at,
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
	
	FROM cleaning_pl
	where notes not in ('{{wrong_notes | join("','") }}')
)
	
SELECT 
	{{ dbt_utils.generate_surrogate_key(["no_packinglist", "no_sj"]) }} as shipment_id,
	fnl_cleaning_pl.*
FROM fnl_cleaning_pl