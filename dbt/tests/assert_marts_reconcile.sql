with source_totals as (
    select
        count() as transaction_count,
        sum(is_fraud) as fraud_count,
        sum(amount) as total_amount
    from {{ ref('stg_transactions') }}
)

{% set marts = [
    'mart_daily_state_metrics',
    'mart_fraud_by_category',
    'mart_fraud_by_state',
    'mart_customer_risk_profile',
    'mart_hourly_fraud_pattern',
    'mart_merchant_analytics'
] %}
{% for mart in marts %}
    select '{{ mart }}' as model_name
    from (
        select
            sum(transaction_count) as transactions,
            sum({{ 'turnover' if mart == 'mart_merchant_analytics' else 'total_amount' }}) as amount,
            {% if mart != 'mart_daily_state_metrics' %}
                sum(fraud_count) as frauds
            {% else %}
                toUInt64(0) as frauds
            {% endif %}
        from {{ ref(mart) }}
    ) as totals
    cross join source_totals
    where
        totals.transactions != source_totals.transaction_count
        or totals.amount != source_totals.total_amount
        {% if mart != 'mart_daily_state_metrics' %}
            or totals.frauds != source_totals.fraud_count
        {% endif %}
    {% if not loop.last %}
        union all
    {% endif %}
{% endfor %}
