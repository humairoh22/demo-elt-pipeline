select 
    transfer_id,
    no_packinglist,
    delivery_man,
    case
        when status_confirm = 'confirm' then 'Confirmed'
        else null
    end as confirm_hub,
    date(confirm_date) as confirm_date,
    TO_CHAR(confirm_date, 'HH24:MI:00') as confirm_time

from {{ source("public", "transferstatus")}}