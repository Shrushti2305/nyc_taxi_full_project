-- Generic test: Validate that trip_duration_minutes is within configurable range
-- Usage: tests: [trip_duration_range: {min_value: 1, max_value: 180}]
{% test trip_duration_range(model, column_name, min_value=1, max_value=180) %}

SELECT *
FROM {{ model }}
WHERE {{ column_name }} < {{ min_value }}
   OR {{ column_name }} > {{ max_value }}

{% endtest %}