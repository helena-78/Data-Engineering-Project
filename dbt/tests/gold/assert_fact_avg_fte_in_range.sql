{#- Value range (Melih): average FTE employees can not be negative, and no Estonian company has
    100,000 or more employees, so a larger value is a unit or parsing error. -#}
select company_year_key, report_id, avg_fte
from {{ ref('fact_company_year') }}
where avg_fte < 0 or avg_fte >= 100000
