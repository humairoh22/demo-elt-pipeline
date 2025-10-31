select 
    do_id,
    {{ clean_whitespace("no_pesan") }} no_pesan,
    {{ clean_whitespace("no_faktur") }} no_faktur,
    id_customer,
    tgl_faktur::date as tgl_faktur
from {{ source("public", "delivery_order") }}