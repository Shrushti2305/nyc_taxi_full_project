SELECT
    hour,
    total_trips,
    avg_fare,
    AVG(total_trips) OVER (
        ORDER BY hour
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) AS rolling_avg
FROM (
    SELECT
        EXTRACT(hour FROM pickup_datetime) AS hour,
        COUNT(*) AS total_trips,
        AVG(fare_amount) AS avg_fare
    FROM fct_trips
    GROUP BY 1
)
ORDER BY hour;
