-- Table: public.trades
-- DROP TABLE IF EXISTS public.trades;
CREATE TABLE IF NOT EXISTS public.trades (
    coin TEXT NOT NULL,
    side TEXT NOT NULL,
    px NUMERIC(18,8) NOT NULL,
    sz NUMERIC(18,8) NOT NULL,
    "time" BIGINT NOT NULL,
    hash TEXT NOT NULL,
    tid BIGINT NOT NULL,
    user_buyer TEXT NOT NULL,
    user_seller TEXT NOT NULL,
    CONSTRAINT trades_pkey PRIMARY KEY (tid, coin, "time")
)
SELECT create_hypertable('trades', 'time');