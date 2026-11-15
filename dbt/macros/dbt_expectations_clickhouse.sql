{#-
  ClickHouse version of dbt_expectations.regexp_instr (owner: Melih).
  dbt_expectations has no ClickHouse implementation; its default renders regexp_instr(), which ClickHouse
  does not have. The regex tests only check "> 0", so returning 1 for a match and 0 otherwise is enough.
  ClickHouse match() uses RE2 syntax. Enabled by the dispatch block in dbt_project.yml.
-#}
{% macro clickhouse__regexp_instr(source_value, regexp, position, occurrence, is_raw, flags) %}
    if(match(toString({{ source_value }}), '{{ regexp }}'), 1, 0)
{% endmacro %}
