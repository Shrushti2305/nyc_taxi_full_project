SELECT *
FROM (
    SELECT
        DATE_TRUNC('month', pickup_datetime) AS month,
        pickup_zone,
        SUM(total_amount) AS revenue,
        RANK() OVER (
            PARTITION BY DATE_TRUNC('month', pickup_datetime)
            ORDER BY SUM(total_amount) DESC
        ) AS rank
    FROM fct_trips
    GROUP BY 1,2
)
WHERE rank <= 10
ORDER BY month, rank;
