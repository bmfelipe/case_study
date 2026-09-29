-- Grano: una transacción válida, identificada por transaction_id en el lote.
-- Cliente desconocido sigue siendo NULL; nunca se sustituye por un ID inventado.
select
    batch_id,
    source_row_number,
    transaction_id,
    customer_id,
    transaction_date,
    product_id,
    quantity,
    unit_price,
    tax_amount,
    (quantity * unit_price)::numeric(28,2) as subtotal_amount,
    (quantity * unit_price + tax_amount)::numeric(28,2) as total_amount,
    quality_warnings
from {{ ref('int_classified_transactions') }}
where cardinality(rejection_reasons) = 0