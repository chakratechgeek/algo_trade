
CREATE TABLE IF NOT EXISTS data.symbols (
  symbol_id      SERIAL PRIMARY KEY,
  symbol         VARCHAR(20) UNIQUE NOT NULL,    -- your internal code (e.g. 'RELIANCE')
  --ticker         VARCHAR(20) NOT NULL,           -- actual market ticker (e.g. 'RELIANCE.NS')
  name           VARCHAR(100),                   -- company full name
  exchange       VARCHAR(10),
  sector         VARCHAR(50),
  isin           VARCHAR(20),
  fno_eligible   BOOLEAN DEFAULT FALSE,
  active         BOOLEAN DEFAULT TRUE,
  metadata       JSONB
);