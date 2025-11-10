{#
  Enhanced StarRocks Type Detection Macro with Dynamic Numeric Detection
  Типы: INTEGER, DECIMAL, DATE, DATETIME, VARCHAR

  Принципы определения:
  - Пустые/NULL столбцы → DECIMAL(38,10)
  - Перед проверкой числовых паттернов удаляем пробелы и NBSP (CHAR(160))
  - INTEGER/DECIMAL: по regex; при наличии дробной части → DECIMAL, иначе → INTEGER
  - DATETIME и DATE: по regex-паттернам (учитываем pandas datetime64)
  - Порог матчинга паттернов: >= 90% от truly_non_empty_count
  - Длина VARCHAR подбирается по максимальной длине
#}

{% macro generate_comprehensive_type_detection_query(table_schema, table_name, columns=none) %}

  {% if columns is none %}
    {% set columns = get_columns_list_by_table(table_schema, table_name) %}
  {% endif %}

  {% set full_table_name = table_schema ~ '.' ~ table_name %}
  {% set queries = [] %}

  {% for column in columns %}
    {# Пропускаем заведомо проблемные имена (содержат обратную кавычку или backslash) #}
    {% if '`' in column or '\\' in column %}
      {% continue %}
    {% endif %}
    {# Экранируем обратные кавычки в именах колонок для StarRocks/MySQL-диалекта #}
    {% set safe_column_for_identifier = column | replace('`', '``') %}
    {% set quoted_column = '`' ~ safe_column_for_identifier ~ '`' %}
    {# Экранируем одинарные кавычки в строковом литерале для field_nm #}
    {% set safe_column_for_literal = column | replace("'", "''") %}
    {# Общее выражение "очистки" значения колонки для числовых проверок #}
    {% set cleaned_value %}
      TRIM(
        REPLACE(
          REPLACE(CAST({{ quoted_column }} AS VARCHAR), ' ', ''),
          CHAR(160),
          ''
        )
      )
    {% endset %}

    {# Регулярные выражения для числовых типов #}
    {% set re_integer = "'^-?[0-9]+$'" %}
    {% set re_decimal = "'^-?[0-9]+(\\.[0-9]+)?$'" %}
    {% set re_fractional = "'^-?[0-9]+\\.[0-9]+$'" %}

    {# Регулярные выражения/паттерны для дат/дат-времен #}
    {% set re_datetime_full = "'^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}'" %}
    {% set re_datetime_short = "'^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}'" %}
    {% set like_pandas_dt1 = "'%-%-%T%:%:%'" %}
    {% set like_pandas_dt2 = "'%-%- %:%:%'" %}
    {% set re_date_iso = "'^[0-9]{4}-[0-9]{2}-[0-9]{2}$'" %}
    {% set re_date_dot = "'^[0-9]{2}\\.[0-9]{2}\\.[0-9]{4}$'" %}
    {% set re_date_slash = "'^[0-9]{2}/[0-9]{2}/[0-9]{4}$'" %}

    {% set column_query %}
      SELECT
        '{{ safe_column_for_literal }}' AS field_nm,

        -- базовые счётчики заполненности
        SUM(CAST({{ quoted_column }} AS VARCHAR) IS NOT NULL) AS not_null_count,
        SUM(CAST({{ quoted_column }} AS VARCHAR) IS NOT NULL
            AND TRIM(CAST({{ quoted_column }} AS VARCHAR)) != '') AS truly_non_empty_count,
        COUNT(*) AS total_rows,

        -- числовые паттерны (после очистки значений)
        SUM(CAST({{ quoted_column }} AS VARCHAR) IS NOT NULL
            AND {{ cleaned_value }} REGEXP {{ re_integer }}) AS integer_count,

        SUM(CAST({{ quoted_column }} AS VARCHAR) IS NOT NULL
            AND {{ cleaned_value }} REGEXP {{ re_decimal }}) AS decimal_count,

        -- Кол-во значений с ненулевой дробной частью
        SUM(CAST({{ quoted_column }} AS VARCHAR) IS NOT NULL
            AND {{ cleaned_value }} REGEXP {{ re_fractional }}
            AND CAST({{ cleaned_value }} AS DECIMAL(38,10))
                != FLOOR(CAST({{ cleaned_value }} AS DECIMAL(38,10)))
        ) AS fractional_values_count,

        -- датавременные паттерны
        SUM(CAST({{ quoted_column }} AS VARCHAR) IS NOT NULL
            AND (
                CAST({{ quoted_column }} AS VARCHAR) REGEXP {{ re_datetime_full }} OR
                CAST({{ quoted_column }} AS VARCHAR) REGEXP {{ re_datetime_short }} OR
                CAST({{ quoted_column }} AS VARCHAR) LIKE {{ like_pandas_dt1 }} OR
                CAST({{ quoted_column }} AS VARCHAR) LIKE {{ like_pandas_dt2 }}
            )
        ) AS datetime_count,

        -- датовые паттерны (без времени)
        SUM(CAST({{ quoted_column }} AS VARCHAR) IS NOT NULL
            AND (
                CAST({{ quoted_column }} AS VARCHAR) REGEXP {{ re_date_iso }} OR
                CAST({{ quoted_column }} AS VARCHAR) REGEXP {{ re_date_dot }} OR
                CAST({{ quoted_column }} AS VARCHAR) REGEXP {{ re_date_slash }}
            )
            AND CAST({{ quoted_column }} AS VARCHAR) NOT LIKE '%:%'
        ) AS date_count,

        -- максимальная длина строкового представления
        MAX(LENGTH(CAST({{ quoted_column }} AS VARCHAR))) AS max_length

      FROM {{ full_table_name }}
    {% endset %}

    {% do queries.append(column_query) %}
  {% endfor %}

  {% if queries | length == 0 %}
    {% set final_query %}
      SELECT
        CAST(NULL AS VARCHAR) as field_nm,
        CAST(NULL AS VARCHAR) as field_type_nm,
        0 as not_null_count,
        0 as truly_non_empty_count,
        0 as integer_count,
        0 as decimal_count,
        0 as fractional_values_count,
        0 as date_count,
        0 as datetime_count,
        0 as max_length,
        0.0 as integer_match_pct,
        0.0 as decimal_match_pct,
        0.0 as fractional_match_pct,
        0.0 as datetime_match_pct
      LIMIT 0
    {% endset %}
  {% else %}
    {% set final_query %}
      SELECT
        field_nm,
        CASE
          -- Empty/NULL columns → DECIMAL (based on truly_non_empty_count = 0)
          WHEN truly_non_empty_count = 0 THEN 'DECIMAL(38,10)'

          -- DATETIME types (check before numeric types to catch datetime64 columns)
          WHEN datetime_count > 0 AND datetime_count = truly_non_empty_count THEN 'DATETIME'

          -- DATE types (pure date columns without time)
          WHEN date_count > 0 AND date_count = truly_non_empty_count THEN 'DATE'

          -- Dynamic numeric type detection based on fractional values presence
          -- If column has numeric values and any fractional parts exist, use DECIMAL
          WHEN decimal_count > 0 AND decimal_count = truly_non_empty_count THEN
            CASE
              WHEN fractional_values_count > 0 THEN 'DECIMAL(38,10)'
              ELSE 'INTEGER'
            END

          -- VARCHAR with optimal length for text/mixed data
          WHEN max_length <= 50 THEN 'VARCHAR(50)'
          WHEN max_length <= 255 THEN CONCAT('VARCHAR(', GREATEST(max_length, 50), ')')
          WHEN max_length <= 1000 THEN CONCAT('VARCHAR(', max_length, ')')
          WHEN max_length <= 65535 THEN CONCAT('VARCHAR(', max_length, ')')
          ELSE 'VARCHAR(65533)'
        END as field_type_nm,
        not_null_count,
        truly_non_empty_count,
        integer_count,
        decimal_count,
        fractional_values_count,
        date_count,
        datetime_count,
        max_length,
        -- Debug ratios to see pattern matching effectiveness
        ROUND(100.0 * integer_count / GREATEST(truly_non_empty_count, 1), 1) as integer_match_pct,
        ROUND(100.0 * decimal_count / GREATEST(truly_non_empty_count, 1), 1) as decimal_match_pct,
        ROUND(100.0 * fractional_values_count / GREATEST(truly_non_empty_count, 1), 1) as fractional_match_pct,
        ROUND(100.0 * datetime_count / GREATEST(truly_non_empty_count, 1), 1) as datetime_match_pct
      FROM (
        {{ queries | join('\n    UNION ALL\n    ') }}
      ) AS type_analysis
      ORDER BY field_nm
    {% endset %}
  {% endif %}

  {{ return(final_query) }}

{% endmacro %}
