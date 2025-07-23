CREATE TABLE IF NOT EXISTS data.market_snapshots_52weeks (
    id                SERIAL PRIMARY KEY,
    symbol_id         INTEGER NOT NULL REFERENCES data.symbols(symbol_id),
    snapshot_ts       TIMESTAMPTZ NOT NULL,         -- Time of snapshot (with timezone)
    last_traded_price NUMERIC(20,6) NOT NULL,
    last_traded_qty   BIGINT,
    average_price     NUMERIC(20,6),
    volume            BIGINT,                       -- volume_trade_for_the_day
    total_buy_qty     BIGINT,
    total_sell_qty    BIGINT,
    open_price        NUMERIC(20,6),
    high_price        NUMERIC(20,6),
    low_price         NUMERIC(20,6),
    close_price       NUMERIC(20,6),
    last_traded_ts    TIMESTAMPTZ,                  -- exchange-provided last trade timestamp
    open_interest     BIGINT,
    oi_change_pct     NUMERIC(12,6),
    upper_circuit     NUMERIC(20,6),
    lower_circuit     NUMERIC(20,6),
    fifty_two_week_high NUMERIC(20,6),
    fifty_two_week_low  NUMERIC(20,6),
    source            VARCHAR(32) NOT NULL DEFAULT 'AngelOne',
    extra_data        JSONB,
    UNIQUE(symbol_id, snapshot_ts)
);

