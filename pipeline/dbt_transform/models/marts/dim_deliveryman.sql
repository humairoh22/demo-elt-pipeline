select 
    {{ dbt_utils.generate_surrogate_key(["deliveryman_name"])}} as sk_deliveryman_id,
    deliveryman_id as nk_deliveryman_id,
    deliveryman_name,
    CASE 
			WHEN deliveryman_name IS NULL OR deliveryman_name = '' THEN null
			WHEN deliveryman_name ILIKE '%Esa Muh%' THEN 'JKT ESA'
			WHEN deliveryman_name ILIKE '%Bayu Sap%' THEN 'JKT BAYU'
			WHEN deliveryman_name ILIKE '%Dimas Dewa%' THEN 'JKT DMAS'
			WHEN deliveryman_name ILIKE '%abdul jal%' THEN 'BDG JLIL'
			WHEN deliveryman_name ILIKE '%fian dwi%' THEN 'BGR FIAN'
			WHEN deliveryman_name ILIKE '%suhendar%' THEN 'CJR NDAR'
			WHEN deliveryman_name ILIKE '%aqiyu%' THEN 'CRB FHSA'
			WHEN deliveryman_name ILIKE '%dicky%' THEN 'LMJ DCKY'
			WHEN deliveryman_name ILIKE '%eko war%' THEN 'SLO EKOW'
			WHEN deliveryman_name ILIKE '%nurul fajar%' THEN 'TSM FJAR'
			WHEN deliveryman_name ILIKE '%petrik%' THEN 'KDR PTRK'
			WHEN deliveryman_name ILIKE '%rendy%' THEN 'SBY RNDY'
			WHEN deliveryman_name ILIKE '%muchtar%' THEN 'SMG MHTR'
			WHEN deliveryman_name ILIKE '%dany apr%' THEN 'JKT DANY'
			WHEN deliveryman_name ILIKE '%rafta%' THEN 'DIY RFTA'
			WHEN deliveryman_name ILIKE '%kukuh%' THEN 'DIY KKUH'
			WHEN deliveryman_name ILIKE '%listyo%' THEN 'DIY LTYO'
			WHEN deliveryman_name ILIKE '%indri%' THEN 'JKT IDRI'
			WHEN deliveryman_name ILIKE '%erika%' THEN 'JKT ERKA'
			WHEN deliveryman_name ILIKE '%aldrien%' THEN 'JKT ALDR'
			WHEN deliveryman_name ILIKE '%indopaket%' THEN 'IDPK'
			WHEN deliveryman_name ILIKE '%21%' THEN '21 EXP'
			WHEN deliveryman_name ILIKE '%cahyadi pur%' THEN 'SBY CHYD'
			WHEN deliveryman_name ILIKE '%eksam%' THEN 'JKT EKSM'
			WHEN deliveryman_name ILIKE '%didit pra%' THEN 'KDR DDIT'
			WHEN deliveryman_name ILIKE '%nandar%' THEN 'CJR NDAR'
			WHEN deliveryman_name ILIKE '%roby husada%' THEN 'SRG ROBY'
			WHEN deliveryman_name ILIKE '%dwi okta%' THEN 'LMJ DWIO'
			WHEN deliveryman_name ILIKE '%yunus%' THEN 'MLG YNUS'
			WHEN deliveryman_name ILIKE '%rangga fredian%' THEN 'KNG RNGA'
			WHEN deliveryman_name ILIKE '%bayu seno%' THEN 'DIY BAYU'
			WHEN deliveryman_name ILIKE '%titus%' THEN 'LMJ TTUS'
			WHEN deliveryman_name ILIKE '%lintang%' THEN 'SDA LNTG'
			WHEN deliveryman_name ILIKE '%rizal%' THEN 'TGR RZAL'
			WHEN deliveryman_name ILIKE '%martua%' THEN 'JKT MRTU'
			WHEN deliveryman_name ILIKE '%retza sap%' THEN 'JKT REZA'
			WHEN deliveryman_name ILIKE '%bambang setyo%' THEN 'JKT MBENG'
			WHEN deliveryman_name ILIKE '%Aang%' THEN 'TGR AANG'
			WHEN deliveryman_name ILIKE '%Rafli%' THEN 'JKT RAFLI'
			WHEN deliveryman_name ILIKE '%joko%' THEN 'DIY JOKO'
			WHEN deliveryman_name ILIKE '%rere%' THEN 'SRG RERE'
			WHEN deliveryman_name ILIKE '%rulli%' THEN 'LBK RULI'
			WHEN deliveryman_name ILIKE '%sabar rica%' THEN 'JKT RICA'
			WHEN deliveryman_name ILIKE '%eko prihar%' THEN 'JKT EKOP'
			WHEN deliveryman_name ILIKE '%ADEX%' THEN 'ADEX'
			ELSE deliveryman_name
		END AS shipper_code,

    {{ dbt_date.now() }} as created_at,
    {{ dbt_date.now() }} as update_at

from {{ ref("stg_deliveryman")}}