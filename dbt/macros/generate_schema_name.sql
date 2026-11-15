{#-
  Layer = ClickHouse database. A custom schema (from +schema in dbt_project.yml or a model config)
  is used exactly as written, so +schema: silver -> database silver, +schema: gold -> database gold.
  Without a custom schema the target schema from profiles.yml is used (DBT_DEFAULT_SCHEMA, default gold).
-#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
