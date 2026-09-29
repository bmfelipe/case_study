-- Grano: UN registro del CSV en el lote fijado por Airflow (o último lote por defecto).
-- Todos los casts están protegidos por regex/rango. Se conserva el valor original.
with input as (
    select
        batch_id,
        source_row_number,
        transaction_id as raw_transaction_id,
        customer_id as raw_customer_id,
        transaction_date as raw_transaction_date,
        product_id as raw_product_id,
        product_name as raw_product_name,
        quantity as raw_quantity,
        price as raw_price,
        tax as raw_tax
    from {{ source('raw', 'customer_transactions') }}
    {% if var('batch_id', none) is not none %}
    where batch_id = {{ var('batch_id') | int }}
    {% else %}
    where batch_id = (select max(batch_id) from {{ source('raw', 'ingestion_batches') }})
    {% endif %}
), normalized as (
    select *,
        nullif(btrim(raw_transaction_id), '') as transaction_id_text,
        nullif(btrim(raw_customer_id), '') as customer_id_text,
        nullif(btrim(raw_transaction_date), '') as date_text,
        upper(nullif(btrim(raw_product_id), '')) as product_id_text,
        nullif(btrim(raw_product_name), '') as product_name_text,
        nullif(btrim(raw_quantity), '') as quantity_text,
        nullif(btrim(raw_price), '') as price_text,
        nullif(btrim(raw_tax), '') as tax_text
    from input
), standardized_dates as (
    select *,
        case
            when date_text ~ '^[1-9][0-9]{3}-(0[1-9]|1[0-2])-(0[1-9]|[12][0-9]|3[01])$'
                then date_text
            when date_text ~ '^(0[1-9]|[12][0-9]|3[01])-(0[1-9]|1[0-2])-[1-9][0-9]{3}$'
                then substr(date_text, 7, 4) || '-' || substr(date_text, 4, 2)
                     || '-' || substr(date_text, 1, 2)
        end as iso_date_text
    from normalized
), dated as (
    select *,
        case when iso_date_text is not null then
            case when substr(iso_date_text, 9, 2)::integer <= extract(day from
                (make_date(substr(iso_date_text, 1, 4)::integer,
                           substr(iso_date_text, 6, 2)::integer, 1) + interval '1 month - 1 day'))
                then make_date(substr(iso_date_text, 1, 4)::integer,
                               substr(iso_date_text, 6, 2)::integer,
                               substr(iso_date_text, 9, 2)::integer)
            end
        end as transaction_date
    from standardized_dates
), typed as (
    select *,
        case when transaction_id_text ~ '^(T)?[0-9]{1,15}$'
            then transaction_id_text end as transaction_id,
        case when customer_id_text ~ '^[0-9]{1,15}([.]0+)?$'
            then split_part(customer_id_text, '.', 1)::bigint end as customer_id,
        case when product_id_text ~ '^P?[0-9]{1,15}$'
            then regexp_replace(product_id_text, '^P', '')::bigint end as product_id,
        initcap(product_name_text) as product_name,
        case when quantity_text ~ '^[0-9]{1,9}([.]0+)?$'
            then split_part(quantity_text, '.', 1)::integer end as quantity,
        case when price_text ~ '^[0-9]{1,12}([.][0-9]{1,2})?$'
            then price_text::numeric(18,2) end as unit_price,
        case when tax_text ~ '^[0-9]{1,12}([.][0-9]{1,2})?$'
            then tax_text::numeric(18,2) end as tax_amount
    from dated
)
select
    batch_id, source_row_number,
    raw_transaction_id, raw_customer_id, raw_transaction_date,
    raw_product_id, raw_product_name, raw_quantity, raw_price, raw_tax,
    transaction_id, customer_id, transaction_date, product_id,
    product_name, quantity, unit_price, tax_amount,
    array_remove(array[
        case when transaction_id is null then 'invalid_transaction_id' end,
        case when customer_id_text is not null and (customer_id is null or customer_id <= 0)
            then 'invalid_customer_id' end,
        case when transaction_date is null then 'invalid_transaction_date' end,
        case when product_id is null or product_id <= 0 then 'invalid_product_id' end,
        case when product_name is null then 'missing_product_name' end,
        case when quantity is null or quantity <= 0 then 'invalid_quantity' end,
        case when unit_price is null or unit_price <= 0 then 'invalid_unit_price' end,
        case when tax_amount is null or tax_amount < 0 then 'invalid_tax_amount' end
    ], null::text) as validation_errors,
    array_remove(array[
        case when customer_id_text is null then 'missing_customer_id' end,
        case when date_text ~ '^[0-9]{2}-' and transaction_date is not null
            then 'dmy_date_normalized' end,
        case when product_id_text ~ '^P[0-9]+$' then 'product_prefix_normalized' end
    ], null::text) as quality_warnings
from typed