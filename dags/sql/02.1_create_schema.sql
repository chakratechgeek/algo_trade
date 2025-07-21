DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
      FROM pg_namespace
     WHERE nspname = 'data'
  )
  THEN
    CREATE SCHEMA data;
  END IF;
END
$$;
