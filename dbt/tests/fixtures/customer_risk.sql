select
    customer_id,
    concat(customer_id, '_', toString(number)) as transaction_id,
    toDateTime('2024-01-01 10:00:00') as transaction_time,
    toDecimal64(if(customer_id = 'large', 1000, 10), 2) as amount,
    toUInt8(
        (customer_id = 'medium' and number = 0)
        or (customer_id = 'high_rate' and number = 0)
        or (customer_id = 'high_count' and number < 3)
    ) as is_fraud
from numbers(100) as counter
cross join (
    select arrayJoin(['low', 'large', 'medium', 'high_rate', 'high_count']) as customer_id
) as clients
where (customer_id = 'low' and number < 2)
    or (customer_id = 'large' and number = 0)
    or (customer_id = 'high_rate' and number < 20)
    or customer_id in ('medium', 'high_count')
