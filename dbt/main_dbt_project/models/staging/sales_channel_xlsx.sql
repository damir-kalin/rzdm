{#
  Шаблон модели для автоматической типизации таблиц
  Переменные:
  - source_schema: схема источника
  - source_table: таблица источника
#}

{{ config(
    materialized='table',
    post_hook=[
        "CREATE OR REPLACE VIEW {{ this.schema }}.v_{{ this.name }} AS SELECT * FROM {{ this }}"
    ]
) }}

{% set source_schema = var('source_schema', 'stage') %}
{% set source_table = var('source_table', 'sandbox_db_hr') %}
{% set full_source_table = source_schema ~ '.' ~ source_table %}

{# Получаем определение типов данных через comprehensive макрос #}
{% set type_detection_query = generate_comprehensive_type_detection_query(source_schema, source_table) %}

{# Выполняем запрос для получения типов #}
{% if execute %}
  {% set type_results = run_query(type_detection_query) %}
  {% set field_types = {} %}
  {% for row in type_results %}
    {% do field_types.update({row[0]: row[1]}) %}
  {% endfor %}
{% else %}
  {% set field_types = {} %}
{% endif %}

{# Генерируем SELECT с правильными типами #}
{% if field_types %}
SELECT
{% for field_name, field_type in field_types.items() %}
  {%- set lower_type = (field_type | lower) -%}
  {%- set is_integer = (lower_type == 'integer') -%}
  {%- set is_decimal = (lower_type[:7] == 'decimal') -%}
  {%- if is_integer -%}
    CAST(
      CAST(
        TRIM(
          REPLACE(
            REPLACE(CAST(`{{ field_name }}` AS VARCHAR), ' ', ''),
            CHAR(160),
            ''
          )
        ) AS DECIMAL(38,10)
      ) AS INTEGER
    ) AS `{{ field_name }}`
  {%- elif is_decimal -%}
    CAST(
      TRIM(
        REPLACE(
          REPLACE(CAST(`{{ field_name }}` AS VARCHAR), ' ', ''),
          CHAR(160),
          ''
        )
      ) AS {{ field_type }}
    ) AS `{{ field_name }}`
  {%- else -%}
    CAST(`{{ field_name }}` AS {{ field_type }}) AS `{{ field_name }}`
  {%- endif -%}
  {%- if not loop.last -%},{%- endif %}
{% endfor %}

FROM {{ full_source_table }}
{% else %}
-- Fallback if no type information available
SELECT * FROM {{ full_source_table }}
{% endif %}