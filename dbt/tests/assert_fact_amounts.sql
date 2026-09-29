select transaction_id
from {{ ref('fact_transactions') }}
where quantity <= 0
   or unit_price <= 0
   or tax_amount < 0
   or (customer_id is not null and customer_id <= 0)
   or subtotal_amount is distinct from quantity * unit_price
   or total_amount is distinct from subtotal_amount + tax_amount