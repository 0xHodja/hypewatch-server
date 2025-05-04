-- Table: public.candles
-- DROP TABLE IF EXISTS public.candles;
CREATE TABLE IF NOT EXISTS public.candles
(
    t bigint NOT NULL,
    s text NOT NULL,
    i text NOT NULL,
    o numeric(18,8) NOT NULL,
    c numeric(18,8) NOT NULL,
    h numeric(18,8) NOT NULL,
    l numeric(18,8) NOT NULL,
    v numeric(18,8) NOT NULL,
    CONSTRAINT candles_pkey PRIMARY KEY (t, s, i)
);
SELECT create_hypertable('candles','t');