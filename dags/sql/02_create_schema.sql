DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
      FROM pg_namespace
     WHERE nspname = 'tracker'
  )
  THEN
    CREATE SCHEMA tracker;
  END IF;
END
$$;
