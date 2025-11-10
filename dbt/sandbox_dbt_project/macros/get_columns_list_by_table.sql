{#
  Макрос для получения списка колонок из таблицы
  Параметры:
  - table_schema: схема таблицы
  - table_name: имя таблицы
#}

{% macro get_columns_list_by_table(table_schema, table_name) %}

  {{ log("DEBUG: table_schema = " ~ table_schema, info=true) }}
  {{ log("DEBUG: table_name = " ~ table_name, info=true) }}

  {% set query %}
    SELECT COLUMN_NAME
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = '{{ table_schema }}'
      AND TABLE_NAME = '{{ table_name }}'
      AND COLUMN_NAME != ''  -- Filter out empty/blank column names without TRIM
    ORDER BY ORDINAL_POSITION
  {% endset %}

  {{ log("DEBUG: query = " ~ query, info=true) }}

  {% set results = run_query(query) %}

  {% if execute %}
    {% set columns = results.columns[0].values() %}
    {{ return(columns) }}
  {% else %}
    {{ return([]) }}
  {% endif %}

{% endmacro %}