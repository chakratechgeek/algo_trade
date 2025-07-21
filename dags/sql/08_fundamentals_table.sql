CREATE TABLE IF NOT EXISTS data.fundamentals (
    fundamental_id SERIAL PRIMARY KEY,
    symbol_id      INT REFERENCES data.symbols(symbol_id),
    period         DATE NOT NULL,           -- e.g., '2024-06-30'
    period_label   VARCHAR(20),             -- e.g., 'Q1FY25', '2024-03'
    filing_date    DATE,                    -- When result was filed/announced
    statement_type VARCHAR(20),             -- 'income', 'balance', 'cashflow'
    metric         text,             -- 'EPS', 'PE', 'Revenue', etc.
    value          NUMERIC(30,6),
    period_type    VARCHAR(10),             -- 'quarter', 'annual'
    currency       VARCHAR(5),
    source         VARCHAR(50),
    extra_data     JSONB NOT NULL DEFAULT '{}',
    UNIQUE(symbol_id, period, metric, source)
);
