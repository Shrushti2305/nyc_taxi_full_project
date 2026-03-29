SELECT *
FROM {{ ref('fct_trips') }}
WHERE total_amount < fare_amount
