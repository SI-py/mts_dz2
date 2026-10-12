SELECT category, count() AS transaction_count, toString(sum(amt)) AS total_amount
FROM homework.transactions
GROUP BY category
ORDER BY transaction_count DESC, category
