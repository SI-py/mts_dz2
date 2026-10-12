SELECT
    state,
    tupleElement(result, 1) AS category,
    tupleElement(result, 2) AS max_amount,
    tupleElement(result, 3) AS trans_num
FROM
(
    SELECT state,
           argMax(tuple(toString(category), amt, trans_num), tuple(amt, trans_num)) AS result
    FROM homework.transactions
    GROUP BY state
)
ORDER BY state
