{{ config(order_by=['day_of_week', 'transaction_hour']) }}

with hourly as (
    select
        day_of_week,
        transaction_hour,
        count() as transaction_count,
        sum(is_fraud) as fraud_count,
        100.0 * sum(is_fraud) / count() as fraud_rate,
        sum(amount) as total_amount,
        sumIf(amount, is_fraud = 1) as fraud_amount
    from {{ ref('stg_transactions') }}
    group by day_of_week, transaction_hour
),

baseline as (
    select 100.0 * sum(is_fraud) / count() as baseline_fraud_rate
    from {{ ref('stg_transactions') }}
)

select
    hourly.*,
    baseline.baseline_fraud_rate,
    hourly.fraud_rate / nullIf(baseline.baseline_fraud_rate, 0) as fraud_rate_lift,
    hourly.fraud_rate > baseline.baseline_fraud_rate as is_elevated_risk
from hourly
cross join baseline
