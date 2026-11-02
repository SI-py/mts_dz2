{{ config(order_by='merchant') }}

with metrics as (
    select
        merchant,
        count() as transaction_count,
        sum(is_fraud) as fraud_count,
        100.0 * sum(is_fraud) / count() as fraud_rate,
        sum(amount) as turnover,
        sumIf(amount, is_fraud = 1) as fraud_amount,
        avg(amount) as average_amount,
        uniqExact(customer_id) as unique_customers,
        uniqExact(state) as states_count
    from {{ ref('stg_transactions') }}
    group by merchant
)

select
    *,
    transaction_count >= {{ var('suspicious_min_transactions') }}
    and fraud_rate >= {{ var('suspicious_fraud_rate') }} as is_suspicious
from metrics
