{% macro clean_whitespace(column_name) %}
    TRIM(
        REGEXP_REPLACE(
            REPLACE(
                REPLACE({{ column_name }}, CHR(160), ' '),
                CHR(9), ' '
            ),
            '\s+', ' ', 'g'
        )
    )
{% endmacro %}