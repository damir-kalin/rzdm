{% macro drop_table_force(relation) %}
  -- Drop object safely, handling both TABLE and VIEW types
  -- StarRocks requires specific handling for different object types
  
  -- For VIEW models, dbt can recreate them directly, so we might not need to drop
  -- But for TABLE models, we need to drop before recreation
  -- We'll try to drop as TABLE first, which is the most common case
  
  -- Try to drop as TABLE with FORCE (handles most cases)
  -- If object doesn't exist or is not a TABLE, this will be ignored
  DROP TABLE IF EXISTS {{ relation }} FORCE;
  
  -- Return dummy select to satisfy dbt
  SELECT 1;
{% endmacro %}

