{% macro generate_alias_name(custom_alias_name=none, node=none) -%}
  {#
    Централизованно задаём имя объекта. Добавляем префикс v_ только
    для VIEW в витринах MART (dims) и REPORT, чтобы не ломать ссылки
    внутри BDV/RDV.
  #}
  {%- set base_alias = custom_alias_name if custom_alias_name is not none else node.name -%}
  {%- set path = node.original_file_path or '' -%}
  {%- if node.config.materialized == 'view' and (
        path.startswith('models/rzdm_mart/dims') or
        path.startswith('models/rzdm_report') or
        path.startswith('models/rzdm_bdv/satellites')
    ) -%}
    {{ return('v_' ~ base_alias) }}
  {%- else -%}
    {{ return(base_alias) }}
  {%- endif -%}
{%- endmacro %}


