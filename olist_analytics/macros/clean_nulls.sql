{% macro clean_string(column_name, default_value='unknown') %}
    coalesce(nullif(trim({{ column_name }}), ''), '{{ default_value }}')
{% endmacro %}