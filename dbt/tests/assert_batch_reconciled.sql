-- dbt falla si se pierde o duplica algún registro en la clasificación.
with input_batch as (
    select batch_id, row_count
    from {{ source('raw', 'ingestion_batches') }}
    {% if var('batch_id', none) is not none %}
    where batch_id = {{ var('batch_id') | int }}
    {% else %}
    where batch_id = (select max(batch_id) from {{ source('raw', 'ingestion_batches') }})
    {% endif %}
)
select batch_id
from input_batch
where row_count <> (select count(*) from {{ ref('stg_transactions') }})
   or row_count <> (select count(*) from {{ ref('fact_transactions') }})
                  + (select count(*) from {{ ref('rejected_transactions') }})

union all

select -1 as batch_id where not exists (select 1 from input_batch)