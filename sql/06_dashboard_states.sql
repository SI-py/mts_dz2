SELECT
    state, transaction_count, toString(total_amount) AS total_amount,
    toString(average_amount) AS average_amount,
    tupleElement(winner, 1) AS category,
    toString(tupleElement(winner, 2)) AS max_amount,
    tupleElement(winner, 3) AS trans_num,
    toString(tupleElement(winner, 4)) AS transaction_time,
    tupleElement(winner, 5) AS merchant
FROM
(
    SELECT state, count() AS transaction_count, sum(amt) AS total_amount,
           avg(amt) AS average_amount,
           argMax(tuple(toString(category), amt, trans_num, trans_time,
                        coalesce(nullIf(JSONExtractString(raw_data, 'merch'), ''),
                                 JSONExtractString(raw_data, 'merchant'))),
                  tuple(amt, trans_num)) AS winner
    FROM homework.transactions
    GROUP BY state
)
ORDER BY state
