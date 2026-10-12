-- Optional report for a single CSV. Pass its SHA-256 as dataset_id.
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
    WHERE dataset_id = {dataset_id:String}
    GROUP BY state
)
ORDER BY state
