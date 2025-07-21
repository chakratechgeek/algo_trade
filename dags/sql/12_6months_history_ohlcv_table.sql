CREATE TABLE IF NOT EXISTS data.ohlcv_history (
    id BIGSERIAL PRIMARY KEY,
    symbol_id INTEGER NOT NULL REFERENCES data.symbols(symbol_id),
    timestamp TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    open NUMERIC(16,4) NOT NULL,
    high NUMERIC(16,4) NOT NULL,
    low NUMERIC(16,4) NOT NULL,
    close NUMERIC(16,4) NOT NULL,
    volume NUMERIC(20,4),
    interval VARCHAR(10) NOT NULL, -- e.g., '1m'
    adjusted BOOLEAN DEFAULT FALSE,
    source VARCHAR(40) NOT NULL,
    extra_data JSONB,
    created_at TIMESTAMP DEFAULT now(),
    updated_at TIMESTAMP DEFAULT now(),
    UNIQUE(symbol_id, interval, timestamp)
);
