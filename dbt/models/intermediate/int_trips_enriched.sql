SELECT
    t.*,
    pz.zone AS pickup_zone,
    dz.zone AS dropoff_zone
FROM {{ ref('stg_yellow_trips') }} t
LEFT JOIN {{ ref('stg_taxi_zones') }} pz
ON t.pu_location_id = pz.location_id
LEFT JOIN {{ ref('stg_taxi_zones') }} dz
ON t.do_location_id = dz.location_id
WHERE trip_distance > 0
AND fare_amount > 0
AND passenger_count > 0
AND trip_duration_minutes BETWEEN 1 AND 180
