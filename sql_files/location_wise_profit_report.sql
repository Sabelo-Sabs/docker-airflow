WITH latest_date AS (
    SELECT MAX(transaction_date) AS max_date
    FROM public.store_transactions
)
SELECT
    t.transaction_date,
    t.store_location,
    ROUND((SUM(t.sp) - SUM(t.cp))::numeric, 2) AS lc_profit
FROM public.store_transactions t
JOIN latest_date d
  ON t.transaction_date = d.max_date
GROUP BY t.transaction_date, t.store_location
ORDER BY lc_profit DESC
LIMIT 50;
