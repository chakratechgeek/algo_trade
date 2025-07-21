CREATE TABLE IF NOT EXISTS data.ohlcv (
    ohlcv_id    SERIAL PRIMARY KEY,
    symbol_id   INT REFERENCES data.symbols(symbol_id),
    timestamp   TIMESTAMPTZ NOT NULL,
    open        NUMERIC(18,6),
    high        NUMERIC(18,6),
    low         NUMERIC(18,6),
    close       NUMERIC(18,6),
    volume      BIGINT,
    interval    VARCHAR(10) NOT NULL,   -- '1m', '5m', '1d', etc.
    adjusted    BOOLEAN DEFAULT FALSE,  -- True if split/dividend adjusted
    source      VARCHAR(50),
    extra_data  JSONB,
    UNIQUE(symbol_id, timestamp, interval, source)
);
