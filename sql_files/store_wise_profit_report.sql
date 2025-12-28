SELECT
  transaction_date AS date,
  store_id,
  ROUND((SUM(sp) - SUM(cp))::numeric, 2) AS st_profit
FROM public.store_transactions
WHERE transaction_date = (CURRENT_DATE - INTERVAL '1 day')::date
GROUP BY transaction_date, store_id
ORDER BY st_profit DESC;
