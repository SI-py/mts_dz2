{% macro amount_bucket(amount_column) -%}
    case
        when {{ amount_column }} < 100 then 'SMALL'
        when {{ amount_column }} < 500 then 'MEDIUM'
        else 'LARGE'
    end
{%- endmacro %}
