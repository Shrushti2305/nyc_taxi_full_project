SELECT
    ROW_NUMBER() OVER (ORDER BY pickup_datetime) AS trip_id,
    *
FROM {{ ref('int_trips_enriched') }}