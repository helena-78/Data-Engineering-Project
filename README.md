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