-- create_store_transactions.sql

CREATE TABLE IF NOT EXISTS public.store_transactions (
  store_id          TEXT NOT NULL,
  store_location    TEXT,
  product_category  TEXT,
  product_id        BIGINT,
  mrp               NUMERIC(12, 2),
  cp                NUMERIC(12, 2),
  discount          NUMERIC(12, 2),
  sp                NUMERIC(12, 2),
  transaction_date  DATE
);

-- Helpful indexes (safe to keep or remove)
CREATE INDEX IF NOT EXISTS idx_store_transactions_date
  ON public.store_transactions (transaction_date);

CREATE INDEX IF NOT EXISTS idx_store_transactions_store_id
  ON public.store_transactions (store_id);
