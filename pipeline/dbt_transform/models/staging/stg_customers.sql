select *
from {{ source("public", "customers")}}