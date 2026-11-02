with normalized as (
    select
        transaction_id,
        transaction_time,
        trimBoth(merch) as merchant,
        lowerUTF8(trimBoth(cat_id)) as category,
        toDecimal64(toString(amount), 2) as amount,
        upperUTF8(trimBoth(gender)) as gender,
        upperUTF8(trimBoth(us_state)) as state,
        toUInt8(target) as is_fraud,
        {{ dbt_utils.generate_surrogate_key([
            'lowerUTF8(trimBoth(name_1))',
            'lowerUTF8(trimBoth(name_2))',
            'lowerUTF8(trimBoth(street))',
            'lowerUTF8(trimBoth(one_city))',
            'upperUTF8(trimBoth(us_state))',
            'trimBoth(post_code)'
        ]) }} as customer_id,
        lat,
        lon,
        merchant_lat,
        merchant_lon
    from {{ source('raw', 'transactions') }}
)

select
    *,
    toDate(transaction_time) as transaction_date,
    toHour(transaction_time) as transaction_hour,
    {{ dbt_date.day_of_week('transaction_time', isoweek=true) }} as day_of_week,
    {{ amount_bucket('amount') }} as amount_bucket,
    amount >= {{ var('large_transaction_amount') }} as is_large_transaction
from normalized
