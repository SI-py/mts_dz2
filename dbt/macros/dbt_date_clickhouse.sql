{% macro clickhouse__day_of_week(date, isoweek=true) -%}
    toDayOfWeek({{ date }}, {{ 0 if isoweek else 3 }})
{%- endmacro %}
