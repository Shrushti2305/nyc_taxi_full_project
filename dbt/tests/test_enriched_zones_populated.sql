-- Test: Enriched trips should have valid zone references
-- Verify that all pickup and dropoff zones are not null after enrichment
SELECT *
FROM {{ ref('int_trips_enriched') }}
WHERE pickup_zone IS NULL
   OR dropoff_zone IS NULL
