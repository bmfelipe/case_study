-- Grano: cliente conocido × mes calendario.
-- Los customer_id NULL aparecen en agg_monthly, pero no se asignan a nadie.
select
    customer_id,
    date_trunc('month', transaction_date)::date as month_start,
    count(*) as transaction_count,
    sum(quantity) as units_sold,
    sum(subtotal_amount)::numeric(28,2) as subtotal_amount,
    sum(tax_amount)::numeric(28,2) as tax_amount,
    sum(total_amount)::numeric(28,2) as total_amount
from {{ ref('fact_transactions') }}
where customer_id is not null
group by 1, 2