{{ config(order_by='state') }}

with metrics as (
    select
        state,
        count() as transaction_count,
        sum(is_fraud) as fraud_count,
        100.0 * sum(is_fraud) / count() as fraud_rate,
        uniqExact(customer_id) as unique_customers,
        uniqExact(merchant) as unique_merchants,
        sum(amount) as total_amount,
        sumIf(amount, is_fraud = 1) as fraud_amount
    from {{ ref('stg_transactions') }}
    group by state
)

select
    metrics.*,
    states.state_name
from metrics
left join {{ ref('states') }} as states on metrics.state = states.state_code
