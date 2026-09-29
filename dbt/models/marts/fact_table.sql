-- Vista de compatibilidad: el contrato oficial es public.fact_transactions.
{{ config(materialized='view') }}

select * from {{ ref('fact_transactions') }}