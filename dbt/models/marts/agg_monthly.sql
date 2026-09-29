-- Grano: mes calendario de la transacción, sin distinguir cliente.
-- Incluye ventas de clientes desconocidos si sus importes son válidos.
select
    date_trunc('month', transaction_date)::date as month_start,
    count(*) as transaction_count,
    count(distinct customer_id) as known_customer_count,
    count(*) filter (where customer_id is null) as unknown_customer_transactions,
    sum(quantity) as units_sold,
    sum(subtotal_amount)::numeric(28,2) as subtotal_amount,
    sum(tax_amount)::numeric(28,2) as tax_amount,
    sum(total_amount)::numeric(28,2) as total_amount
from {{ ref('fact_transactions') }}
group by 1