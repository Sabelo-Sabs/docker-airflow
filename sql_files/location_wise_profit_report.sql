SELECT
  transaction_date AS date,
  store_location,
  ROUND((SUM(sp) - SUM(cp))::numeric, 2) AS lc_profit
FROM public.store_transactions
WHERE transaction_date = (CURRENT_DATE - INTERVAL '1 day')::date
GROUP BY transaction_date, store_location
ORDER BY lc_profit DESC;
