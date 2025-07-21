DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_database WHERE datname='data_collection') THEN
    CREATE DATABASE data_collection;
  END IF;
END
$$;
