{% macro cast_brt_timestamp(column_name) %}
    CONVERT_TIMEZONE('America/Sao_Paulo', TO_TIMESTAMP_NTZ({{ column_name }}))
{% endmacro %}
