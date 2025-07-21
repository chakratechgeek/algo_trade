CREATE TABLE IF NOT EXISTS data.trade_ticks (
    tick_id      BIGSERIAL PRIMARY KEY,
    symbol_id    INTEGER NOT NULL REFERENCES data.symbols(symbol_id),
    timestamp    TIMESTAMPTZ NOT NULL,
    price        NUMERIC(20,6) NOT NULL,
    quantity     BIGINT NOT NULL,
    trade_type   VARCHAR(8), -- 'BUY'/'SELL' if available
    trade_id     VARCHAR(32), -- exchange-provided trade id, if any
    buyer_code   VARCHAR(16),
    seller_code  VARCHAR(16),
    source       VARCHAR(32) NOT NULL DEFAULT 'AngelOne',
    extra_data   JSONB
);
