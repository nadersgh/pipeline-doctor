-- A single payment above $1,000 has never happened in this shop; it signals a unit or logic bug.
select payment_id, amount
from {{ ref('stg_payments') }}
where amount > 1000
