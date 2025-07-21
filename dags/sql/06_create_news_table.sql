CREATE TABLE IF NOT EXISTS data.news (
    news_id          SERIAL PRIMARY KEY,
    symbol_id        INT REFERENCES data.symbols(symbol_id),
    timestamp        TIMESTAMPTZ NOT NULL,
    headline         TEXT,
    body             TEXT,
    sentiment        VARCHAR(10),      -- 'Positive', 'Negative', etc.
    sentiment_score  NUMERIC(6,2),
    news_type        VARCHAR(20),      -- 'news', 'social', 'analyst', etc.
    source           VARCHAR(50),
    url              TEXT,
    extra_data       JSONB
);
