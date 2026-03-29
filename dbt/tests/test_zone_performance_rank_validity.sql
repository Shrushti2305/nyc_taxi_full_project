-- Test: Zone performance revenue rank should be sequential within each month
-- Verify that revenue_rank doesn't have gaps (1,2,3... not 1,3,5...)
SELECT 
    trip_month,
    pickup_zone,
    revenue_rank,
    COUNT(*) as rank_count
FROM {{ ref('agg_zone_performance') }}
GROUP BY 1, 2, 3
HAVING COUNT(*) > 1
