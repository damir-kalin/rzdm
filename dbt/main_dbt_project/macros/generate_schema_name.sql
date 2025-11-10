{% macro generate_schema_name(custom_schema_name=none, node=none) -%}
  {#
    Фиксируем схему по расположению модели, чтобы ref() между слоями
    (RDV/BDV/MART/REPORT) всегда указывал на верную схему независимо от target.
  #}
  {%- set path = node.original_file_path or '' -%}

  {%- if path.startswith('models/rzdm_rdv/') -%}
    {{ return('main_rdv') }}
  {%- elif path.startswith('models/rzdm_bdv/') -%}
    {{ return('main_bdv') }}
  {%- elif path.startswith('models/rzdm_mart/') -%}
    {{ return('main_mart') }}
  {%- elif path.startswith('models/rzdm_report/') -%}
    {{ return('main_report') }}
  {%- else -%}
    {# Fallback: стандартная логика — custom_schema_name или target.schema #}
    {%- set default_schema = custom_schema_name if custom_schema_name is not none else target.schema -%}
    {{ return(default_schema) }}
  {%- endif -%}
{%- endmacro %}


