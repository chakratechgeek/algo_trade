DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
      FROM information_schema.tables
     WHERE table_schema = 'tracker'
       AND table_name   = 'data_collection_tracker'
  )
  THEN
    CREATE TABLE tracker.data_collection_tracker (
      tracker_id         SERIAL PRIMARY KEY,
      data_type          VARCHAR(50) NOT NULL,
      start_date         DATE,
      start_time         TIMESTAMPTZ,
      completion_date    DATE,
      completion_time    TIMESTAMPTZ,
      status             VARCHAR(20) DEFAULT 'PENDING',
      frequency_minutes  INT NOT NULL,
      notes              TEXT,
      record_count       INT,
      error_message      TEXT,
      run_id             UUID DEFAULT gen_random_uuid(),
      duration_seconds   INT,
      initiated_by       VARCHAR(50),
      api_source         VARCHAR(50),
      extra_metadata     JSONB
    );
  END IF;
END
$$;
