-- Star schema for the Estonian company performance project (PostgreSQL)
-- Matches Section 5 (Data Model) and Section 6 (Data Dictionary) of the report.
-- All amounts are in EUR, non-consolidated annual reports. NULL = not reported.

CREATE TABLE dim_year (
    year_key    INTEGER PRIMARY KEY,
    year        SMALLINT NOT NULL UNIQUE          -- calendar year, 2022 to 2025
);

CREATE TABLE dim_industry (
    industry_key    INTEGER PRIMARY KEY,
    section_code    VARCHAR(1)   NOT NULL,        -- EMTAK section letter A to U
    section_name    VARCHAR(255) NOT NULL,
    emtak_version   VARCHAR(20)  NOT NULL,        -- e.g. EMTAK 2008, EMTAK 2025
    UNIQUE (section_code, emtak_version)
);

CREATE TABLE dim_size_band (
    size_band_key   INTEGER PRIMARY KEY,
    band_label      VARCHAR(50) NOT NULL,         -- e.g. Micro (<10), Small (10-49)
    sort_order      SMALLINT    NOT NULL UNIQUE
);

-- SCD Type 2: one row per version of a company
CREATE TABLE dim_company (
    company_key     INTEGER PRIMARY KEY,
    registry_code   VARCHAR(8)   NOT NULL,        -- natural key
    company_name    VARCHAR(255) NOT NULL,
    legal_type      VARCHAR(50),
    county          VARCHAR(50),
    valid_from      DATE NOT NULL,
    valid_to        DATE NOT NULL DEFAULT '9999-12-31',
    is_current      BOOLEAN NOT NULL DEFAULT TRUE
);
CREATE INDEX idx_dim_company_registry ON dim_company (registry_code);

-- Grain: one row per company per calendar year, from the single selected annual report
CREATE TABLE fact_company_year (
    company_year_key    INTEGER PRIMARY KEY,
    company_key         INTEGER NOT NULL REFERENCES dim_company (company_key),
    year_key            INTEGER NOT NULL REFERENCES dim_year (year_key),
    industry_key        INTEGER NOT NULL REFERENCES dim_industry (industry_key),
    size_band_key       INTEGER NOT NULL REFERENCES dim_size_band (size_band_key),
    report_id           VARCHAR(20) NOT NULL,     -- degenerate dimension, lineage to RIK
    revenue             NUMERIC(18,2),
    operating_profit    NUMERIC(18,2),
    assets              NUMERIC(18,2),
    equity              NUMERIC(18,2),
    cash                NUMERIC(18,2),
    current_assets      NUMERIC(18,2),
    current_liabilities NUMERIC(18,2),
    employee_expense    NUMERIC(18,2),
    avg_fte             NUMERIC(10,2),
    UNIQUE (company_key, year_key)
);
CREATE INDEX idx_fact_year     ON fact_company_year (year_key);
CREATE INDEX idx_fact_industry ON fact_company_year (industry_key);
CREATE INDEX idx_fact_size     ON fact_company_year (size_band_key);

-- Seed the static dimensions
INSERT INTO dim_year (year_key, year) VALUES (1, 2022), (2, 2023), (3, 2024), (4, 2025);

INSERT INTO dim_size_band (size_band_key, band_label, sort_order) VALUES
    (1, 'Micro (<10)',     1),
    (2, 'Small (10-49)',   2),
    (3, 'Medium (50-249)', 3),
    (4, 'Large (250+)',    4);
