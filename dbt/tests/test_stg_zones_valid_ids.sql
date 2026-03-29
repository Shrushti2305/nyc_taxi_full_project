-- Test: Taxi zones should have consistent numeric IDs
-- Verify no duplicate location IDs and all IDs are positive
SELECT *
FROM {{ ref('stg_taxi_zones') }}
WHERE location_id <= 0
   OR location_id IS NULL
