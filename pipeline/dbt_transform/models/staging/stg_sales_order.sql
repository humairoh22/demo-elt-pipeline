SELECT 
    {{ clean_whitespace("no_pesan") }} as no_pesan,
    no_po,
    id_customer,
    date(tgl_pesan) as tgl_pesan,
    date(po_expired) as po_expired,
    total_harga
from {{source("public", "sales_order")}}
