-- Test that revenue aggregations have positive values
{% test revenue_is_positive_or_zero(model, column_name) %}

SELECT *
FROM {{ model }}
WHERE {{ column_name }} < 0

{% endtest %}

-- Test that trip counts are positive integers
{% test trip_count_is_positive(model, column_name) %}

SELECT *
FROM {{ model }}
WHERE {{ column_name }} < 1

{% endtest %}
