-- Grano: un producto con, al menos, una transacción aceptada en el lote.
-- int_classified rechaza cualquier product_id con nombres contradictorios.
select
    product_id,
    min(product_name) as product_name
from {{ ref('int_classified_transactions') }}
where cardinality(rejection_reasons) = 0
group by product_id