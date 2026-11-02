{% macro risk_level(fraud_count, fraud_rate, max_amount) -%}
    case
        when {{ fraud_count }} >= 3 or {{ fraud_rate }} >= 5 then 'HIGH'
        when {{ fraud_count }} > 0 or {{ max_amount }} >= 1000 then 'MEDIUM'
        else 'LOW'
    end
{%- endmacro %}
