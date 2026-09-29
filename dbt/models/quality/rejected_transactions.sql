-- Cuándo, dónde y por qué se excluyó la fila, con su contenido ORIGINAL.
select
    batch_id, source_row_number, rejection_reasons, quality_warnings,
    raw_transaction_id, raw_customer_id, raw_transaction_date,
    raw_product_id, raw_product_name, raw_quantity, raw_price, raw_tax
from {{ ref('int_classified_transactions') }}
where cardinality(rejection_reasons) > 0