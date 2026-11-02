{{ config(order_by='customer_id') }}

with metrics as (
    select
        customer_id,
        count() as transaction_count,
        sum(is_fraud) as fraud_count,
        100.0 * sum(is_fraud) / count() as fraud_rate,
        sum(amount) as total_amount,
        avg(amount) as average_amount,
        max(amount) as max_amount,
        min(transaction_time) as first_transaction_time,
        max(transaction_time) as last_transaction_time,
        dateDiff('day', min(transaction_time), max(transaction_time)) as history_days
    from {{ ref('stg_transactions') }}
    group by customer_id
)

select
    *,
    {{ risk_level('fraud_count', 'fraud_rate', 'max_amount') }} as risk_level
from metrics
