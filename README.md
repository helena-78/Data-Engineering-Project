# Data-Engineering-Project

## Data Model — Star Schema (dbdiagram.io DBML)

The star schema below defines the fact table and its four dimensions for the
Estonian Company Performance Observatory. This DBML was written for
dbdiagram.io to generate the Section 5 diagram in the report.

```dbml
Table dim_company {
 company_key int [pk]
 registry_code varchar
 company_name varchar
 legal_type varchar
 county varchar
 valid_from date
 valid_to date
 is_current boolean

 Note: 'SCD Type 2 - name, legal type, county can change over time'
}

Table dim_year {
 year_key int [pk]
 year int

 Note: 'Static - a calendar year never changes'
}

Table dim_industry {
 industry_key int [pk]
 section_code varchar
 section_name varchar
 emtak_version varchar

 Note: 'Static within classification version - EMTAK codes are fixed definitions'
}

Table dim_size_band {
 size_band_key int [pk]
 band_label varchar
 sort_order int

 Note: 'Type 1 - band thresholds are a business rule, overwritten if redefined'
}

Table fact_company_year {
 company_year_key int [pk]
 company_key int [ref: > dim_company.company_key]
 year_key int [ref: > dim_year.year_key]
 industry_key int [ref: > dim_industry.industry_key]
 size_band_key int [ref: > dim_size_band.size_band_key]
 report_id varchar
 revenue decimal
 operating_profit decimal
 assets decimal
 equity decimal
 cash decimal
 current_assets decimal
 current_liabilities decimal
 employee_expense decimal
 avg_fte decimal

 Note: 'Grain: one row per company per calendar year (one selected report per company-year)'
}
```
## SQL

PostgreSQL. Table definitions follow the data dictionary in Section 6 of the report.

| File | Purpose |
|---|---|
| `sql/ddl/01_create_star_schema.sql` | CREATE TABLE statements for the fact table and four dimensions, plus seed rows for `dim_year` and `dim_size_band` |
| `sql/queries/q1_median_revenue_growth_by_industry.sql` | Q1. Industries with the highest median revenue growth |
| `sql/queries/q2_median_operating_margin_by_industry.sql` | Q2. Industries with the highest median operating margin |
| `sql/queries/q3_revenue_per_employee_by_size_band.sql` | Q3. Revenue per employee by employee size band |
| `sql/queries/q4_median_operating_margin_by_size_band.sql` | Q4. Median operating margin by company size |
| `sql/queries/q5_revenue_up_margin_down.sql` | Q5. Companies that grew revenue while their margin declined |

Query conventions, following the KPI rules in Section 1: every ratio requires a positive denominator, sector comparisons use medians, every result shows its sample size, and groups with fewer than 30 companies are excluded. Year over year queries (Q1, Q5) partition by `registry_code` rather than `company_key`, because `dim_company` is SCD Type 2 and a company can receive a new surrogate key when its attributes change.
