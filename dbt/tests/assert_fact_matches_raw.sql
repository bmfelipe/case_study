-- Compara la salida con el origen, no solo con fórmulas dentro del propio mart.
-- Es una comprobación independiente frente a errores en la selección/coerción.
select f.transaction_id
from {{ ref('fact_transactions') }} f
left join {{ source('raw', 'customer_transactions') }} r
    on r.batch_id = f.batch_id and r.source_row_number = f.source_row_number
where r.batch_id is null
   or f.transaction_id is distinct from btrim(r.transaction_id)
   or f.quantity is distinct from case
       when btrim(r.quantity) ~ '^[0-9]{1,9}([.]0+)?$'
           then split_part(btrim(r.quantity), '.', 1)::integer end
   or f.unit_price is distinct from case
       when btrim(r.price) ~ '^[0-9]{1,12}([.][0-9]{1,2})?$'
           then btrim(r.price)::numeric(18,2) end
   or f.tax_amount is distinct from case
       when btrim(r.tax) ~ '^[0-9]{1,12}([.][0-9]{1,2})?$'
           then btrim(r.tax)::numeric(18,2) end
   or not case
       when btrim(r.transaction_date) ~ '^[1-9][0-9]{3}-' then
           to_char(f.transaction_date, 'YYYY-MM-DD') = btrim(r.transaction_date)
       when btrim(r.transaction_date) ~ '^[0-9]{2}-' then
           to_char(f.transaction_date, 'DD-MM-YYYY') = btrim(r.transaction_date)
       else false end