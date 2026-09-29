{% macro generate_schema_name(custom_schema_name, node) -%}
    {# public es un contrato externo explícito; los demás esquemas mantienen
       el prefijo analytics_ de dbt para aislar staging/marts/quality. #}
    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- elif custom_schema_name | trim == 'public' -%}
        public
    {%- else -%}
        {{ default_schema }}_{{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}