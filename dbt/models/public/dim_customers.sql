-- Grano: un cliente conocido con al menos una transacción válida en el lote.
-- El CSV no incluye nombre/email: solo se publica información verificable.
select
    customer_id,
    min(transaction_date) as first_transaction_date,
    max(transaction_date) as last_transaction_date
from {{ ref('fact_transactions') }}
where customer_id is not null
group by customer_id