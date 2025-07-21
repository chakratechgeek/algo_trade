CREATE TABLE IF NOT EXISTS data.orderbook_levels (
    id           SERIAL PRIMARY KEY,
    symbol_id INTEGER NOT NULL REFERENCES data.symbols(symbol_id),
    snapshot_ts  TIMESTAMPTZ   NOT NULL,   -- When the snapshot was taken (with timezone)
    side         CHAR(1)       NOT NULL,   -- 'B' = bid, 'A' = ask
    level        SMALLINT      NOT NULL,   -- 1=best, 2=next, 3=third, etc.
    price        NUMERIC(20,6) NOT NULL,   -- Price at this level
    size         BIGINT        NOT NULL,   -- Size/quantity at this level
    source       VARCHAR(32)   NOT NULL DEFAULT 'AngelOne',
    extra_data   JSONB,
    UNIQUE(symbol_id, snapshot_ts, side, level)
);
