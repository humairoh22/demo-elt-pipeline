select *
from {{ source("public", "delivery_man")}}
where is_deleted = False