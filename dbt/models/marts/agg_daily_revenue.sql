SELECT
    DATE(pickup_datetime) AS trip_date,
    COUNT(*) AS total_trips,
    SUM(fare_amount) AS total_fare,
    AVG(fare_amount) AS avg_fare,
    SUM(tip_amount) AS total_tips,
    ROUND(100.0 * SUM(tip_amount) / NULLIF(SUM(total_amount), 0), 2) AS tip_rate_percent,
    SUM(total_amount) AS total_revenue
FROM {{ ref('fct_trips') }}
GROUP BY 1
ORDER BY 1 DESC