CREATE TABLE IF NOT EXISTS data.risk_events (
    id              SERIAL PRIMARY KEY,
    event_type      VARCHAR(64) NOT NULL,         -- e.g., 'circuit_breaker', 'auction_trigger', 'abnormal_volatility'
    exchange        VARCHAR(32) NOT NULL,         -- e.g., 'NSE', 'BSE', 'NASDAQ'
    symbol          VARCHAR(32),                  -- e.g., 'RELIANCE', 'NIFTY 50', can be NULL for market-wide events
    event_time      TIMESTAMP NOT NULL,           -- when the event occurred (from exchange, not ingestion)
    event_details   JSONB,                        -- raw/details for future-proofing (reason, threshold, notes, etc.)
    ingestion_time  TIMESTAMP DEFAULT now(),      -- when ingested by your ETL
    UNIQUE(exchange, event_type, symbol, event_time)
);
