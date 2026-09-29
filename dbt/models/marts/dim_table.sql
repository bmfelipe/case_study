-- Vista de compatibilidad: el contrato oficial es public.dim_product.
{{ config(materialized='view') }}

select * from {{ ref('dim_product') }}