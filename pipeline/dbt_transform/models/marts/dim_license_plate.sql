select 
    {{dbt_utils.generate_surrogate_key(["license_id", "licenseplate_number"])}} as sk_license_id,
    license_id,
    licenseplate_number as license_plate,
    owner_name,
    {{ dbt_date.now() }} as created_at,
    {{ dbt_date.now() }} as update_at

from {{ ref("stg_license_plate") }}