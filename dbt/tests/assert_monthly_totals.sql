with recomputed as (
    select
        date_trunc('month', transaction_date)::date as month_start,
        count(*) as transaction_count,
        count(distinct customer_id) as known_customer_count,
        count(*) filter (where customer_id is null) as unknown_customer_transactions,
        sum(quantity) as units_sold,
        sum(subtotal_amount) as subtotal_amount,
        sum(tax_amount) as tax_amount,
        sum(total_amount) as total_amount
    from {{ ref('fact_transactions') }}
    group by 1
)
select coalesce(expected.month_start, actual.month_start) as month_start
from recomputed expected
full outer join {{ ref('agg_monthly') }} actual using (month_start)
where expected.month_start is null or actual.month_start is null
   or expected.transaction_count is distinct from actual.transaction_count
   or expected.known_customer_count is distinct from actual.known_customer_count
   or expected.unknown_customer_transactions is distinct from actual.unknown_customer_transactions
   or expected.units_sold is distinct from actual.units_sold
   or expected.subtotal_amount is distinct from actual.subtotal_amount
   or expected.tax_amount is distinct from actual.tax_amount
   or expected.total_amount is distinct from actual.total_amount