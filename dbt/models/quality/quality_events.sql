-- Grano: evento de calidad por registro (una fila puede tener varios eventos).
select
    batch_id, source_row_number, 'error'::text as severity, issue_code
from {{ ref('int_classified_transactions') }}
cross join lateral unnest(rejection_reasons) as events(issue_code)

union all

select
    batch_id, source_row_number, 'warning'::text as severity, issue_code
from {{ ref('int_classified_transactions') }}
cross join lateral unnest(quality_warnings) as events(issue_code)