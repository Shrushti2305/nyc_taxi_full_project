WITH trips AS (
    SELECT
        pickup_zone,
        DATE(pickup_datetime) AS trip_date,
        pickup_datetime,
        dropoff_datetime,
        LAG(dropoff_datetime) OVER (
            PARTITION BY pickup_zone, DATE(pickup_datetime)
            ORDER BY pickup_datetime
        ) AS prev_dropoff
    FROM fct_trips
)
SELECT
    trip_date,
    MAX(DATEDIFF('minute', prev_dropoff, pickup_datetime)) AS max_gap_minutes
FROM trips
WHERE prev_dropoff IS NOT NULL
GROUP BY trip_date
ORDER BY trip_date;
