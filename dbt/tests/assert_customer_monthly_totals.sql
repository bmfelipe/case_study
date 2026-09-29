with recomputed as (
    select
        customer_id,
        date_trunc('month', transaction_date)::date as month_start,
        count(*) as transaction_count,
        sum(quantity) as units_sold,
        sum(subtotal_amount) as subtotal_amount,
        sum(tax_amount) as tax_amount,
        sum(total_amount) as total_amount
    from {{ ref('fact_transactions') }}
    where customer_id is not null
    group by 1, 2
), compared as (
    select
        expected.customer_id as expected_customer_id,
        actual.customer_id as actual_customer_id,
        expected.transaction_count as expected_count,
        actual.transaction_count as actual_count,
        expected.units_sold as expected_units,
        actual.units_sold as actual_units,
        expected.subtotal_amount as expected_subtotal,
        actual.subtotal_amount as actual_subtotal,
        expected.tax_amount as expected_tax,
        actual.tax_amount as actual_tax,
        expected.total_amount as expected_total,
        actual.total_amount as actual_total
    from recomputed expected
    full outer join {{ ref('agg_customer_monthly') }} actual
        on expected.customer_id = actual.customer_id
       and expected.month_start = actual.month_start
)
select coalesce(expected_customer_id, actual_customer_id) as customer_id
from compared
where expected_customer_id is null or actual_customer_id is null
   or expected_count is distinct from actual_count
   or expected_units is distinct from actual_units
   or expected_subtotal is distinct from actual_subtotal
   or expected_tax is distinct from actual_tax
   or expected_total is distinct from actual_total

union all

select customer_id
from {{ ref('agg_customer_monthly') }}
group by customer_id, month_start
having count(*) > 1