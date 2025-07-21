CREATE TABLE IF NOT EXISTS data.mf_navs (
    id           SERIAL PRIMARY KEY,
    scheme_code  INTEGER NOT NULL,
    scheme_name  VARCHAR(256) NOT NULL,
    nav_date     DATE NOT NULL,
    nav          NUMERIC(20,6) NOT NULL,
    repurchase   NUMERIC(20,6),
    sale_price   NUMERIC(20,6),
    extra_data   JSONB,
    UNIQUE(scheme_code, nav_date)
);
