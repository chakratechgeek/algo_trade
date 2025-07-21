CREATE TABLE IF NOT EXISTS data.macro_sector (
    macro_id       SERIAL PRIMARY KEY,
    metric         VARCHAR(1000),
    timestamp      TIMESTAMPTZ NOT NULL,
    value          NUMERIC(30,6),
    unit           VARCHAR(100),
    source         VARCHAR(500),
    tags           TEXT[] DEFAULT '{}',
    extra_data     JSONB NOT NULL DEFAULT '{}'
);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint c
        JOIN pg_class t ON c.conrelid = t.oid
        JOIN pg_namespace n ON t.relnamespace = n.oid
        WHERE c.contype = 'u'  -- unique constraint
          AND t.relname = 'macro_sector'
          AND n.nspname = 'data'
          AND c.conname = 'macro_sector_unique'
    ) THEN
        ALTER TABLE data.macro_sector
        ADD CONSTRAINT macro_sector_unique UNIQUE (metric, timestamp, source);
    END IF;
END
$$;
