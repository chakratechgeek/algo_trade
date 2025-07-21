CREATE TABLE IF NOT EXISTS data.regulatory_events (
    reg_id SERIAL PRIMARY KEY,
    symbol_id INT REFERENCES data.symbols(symbol_id),
    event_time TIMESTAMPTZ NOT NULL,
    event_type VARCHAR(30),       -- e.g. 'InsiderTrade', 'Filing', 'Halt'
    details TEXT,
    status VARCHAR(10),
    source VARCHAR(50),
    extra_data JSONB,
    UNIQUE(symbol_id, event_time, event_type, source)
);
