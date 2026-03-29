SELECT
    tpep_pickup_datetime AS pickup_datetime,
    tpep_dropoff_datetime AS dropoff_datetime,
    passenger_count,
    trip_distance,
    PULocationID AS pu_location_id,
    DOLocationID AS do_location_id,
    fare_amount,
    tip_amount,
    total_amount,
    CAST(DATEDIFF('minute', tpep_pickup_datetime, tpep_dropoff_datetime) AS INTEGER) AS trip_duration_minutes
FROM raw_yellow_trips
WHERE tpep_dropoff_datetime >= tpep_pickup_datetime