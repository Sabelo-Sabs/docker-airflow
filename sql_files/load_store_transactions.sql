COPY public.store_transactions (
  store_id,
  store_location,
  product_category,
  product_id,
  mrp,
  cp,
  discount,
  sp,
  transaction_date
)
FROM '/store_files_postgres/cleaned/clean_store_transactions.csv'
WITH (FORMAT csv, HEADER true);
