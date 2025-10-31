select
    {{dbt_utils.generate_surrogate_key(["customer_id"])}} as sk_customer_id,
    customer_id as nk_customer_id,
    customer_name,
    address as customer_address,
    city,
    province,
    {{ dbt_date.now() }} as created_at,
    {{ dbt_date.now() }} as update_at

from {{ ref("stg_customers") }}