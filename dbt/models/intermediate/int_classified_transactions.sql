-- Se rechazan TODOS los transaction_id duplicados; no hay elección arbitraria.
-- Una discrepancia de nombres para un mismo producto invalida sus filas.
with staged as (
    select * from {{ ref('stg_transactions') }}
), product_names as (
    select product_id
    from staged
    where product_id is not null and product_name is not null
    group by product_id
    having count(distinct product_name) > 1
), classified as (
    select
        staged.*,
        count(*) over (partition by staged.transaction_id) as transaction_id_occurrences,
        (product_names.product_id is not null) as conflicting_product_name
    from staged
    left join product_names on product_names.product_id = staged.product_id
)
select
    *,
    array_remove(
        validation_errors || array[
            case when transaction_id_occurrences > 1 then 'duplicate_transaction_id' end,
            case when conflicting_product_name then 'conflicting_product_name' end
        ],
        null::text
    ) as rejection_reasons
from classified