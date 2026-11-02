select
    transaction_date,
    state
from {{ ref('mart_daily_state_metrics') }}
where
    transaction_count = 0
    or large_transaction_count > transaction_count
    or large_transaction_share < 0
    or large_transaction_share > 100
    or p95_amount < 0
