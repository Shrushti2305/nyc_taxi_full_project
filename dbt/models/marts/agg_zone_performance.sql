WITH zone_monthly AS (
    SELECT
        pickup_zone,
        DATE_TRUNC('month', pickup_datetime) AS trip_month,
        COUNT(*) AS total_trips,
        AVG(trip_distance) AS avg_trip_distance,
        AVG(fare_amount) AS avg_fare,
        SUM(total_amount) AS total_revenue
    FROM {{ ref('fct_trips') }}
    GROUP BY 1, 2
)
SELECT
    pickup_zone,
    trip_month,
    total_trips,
    avg_trip_distance,
    avg_fare,
    total_revenue,
    -- Ranking within each month by revenue (more useful: see top zones each month)
    -- This reveals seasonal patterns and helps marketing target high-performing zones monthly
    RANK() OVER (
        PARTITION BY trip_month
        ORDER BY total_revenue DESC
    ) AS revenue_rank,
    -- Flag zones with high volume (> 10,000 trips per month)
    -- Useful for resource planning and identifying peak demand zones
    CASE WHEN total_trips > 10000 THEN true ELSE false END AS high_volume_zone
FROM zone_monthly
ORDER BY trip_month DESC, revenue_rank ASC
