-- Category of the SINGLE largest transaction per state, not max category total.
-- For equal amounts, choose the lexicographically largest transaction ID.
SELECT
    state,
    tupleElement(result, 1) AS category,
    tupleElement(result, 2) AS max_amount,
    tupleElement(result, 3) AS trans_num
FROM
(
    SELECT state, argMaxMerge(winner) AS result
    FROM homework.max_by_state
    GROUP BY state
)
ORDER BY state
