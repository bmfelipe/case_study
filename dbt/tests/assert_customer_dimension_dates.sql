-- La dimensión no puede inventar clientes ni fechas fuera de sus hechos.
with expected as (
    select
        customer_id,
        min(transaction_date) as first_transaction_date,
        max(transaction_date) as last_transaction_date
    from {{ ref('fact_transactions') }}
    where customer_id is not null
    group by customer_id
)
select coalesce(expected.customer_id, actual.customer_id) as customer_id
from expected
full outer join {{ ref('dim_customers') }} actual using (customer_id)
where expected.customer_id is null or actual.customer_id is null
   or expected.first_transaction_date is distinct from actual.first_transaction_date
   or expected.last_transaction_date is distinct from actual.last_transaction_date
   or actual.first_transaction_date > actual.last_transaction_date