CREATE TABLE IF NOT EXISTS data.corporate_actions (
    action_id           SERIAL PRIMARY KEY,
    symbol_id           INT REFERENCES data.symbols(symbol_id),
    isin                VARCHAR(15),
    ex_date             DATE NOT NULL,
    record_date         DATE,
    book_closure_start  DATE,
    book_closure_end    DATE,
    action_type         VARCHAR(20),
    value               NUMERIC(18,6),
    details             TEXT,
    source              VARCHAR(50),
    extra_data          JSONB,
    UNIQUE(symbol_id, ex_date, action_type, source)
);
