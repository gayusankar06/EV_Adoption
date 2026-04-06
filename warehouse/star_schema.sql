-- ============================================
-- DATABASE
-- ============================================
CREATE DATABASE IF NOT EXISTS ev_warehouse;
USE ev_warehouse;

-- ============================================
-- DIMENSION: VEHICLE
-- ============================================
CREATE EXTERNAL TABLE IF NOT EXISTS dim_vehicle (
    vehicle_id INT,
    make STRING,
    model STRING,
    electric_vehicle_type STRING
)
STORED AS PARQUET
LOCATION '/data_lake/gold/ev_model_popularity';

-- ============================================
-- DIMENSION: LOCATION
-- ============================================
CREATE EXTERNAL TABLE IF NOT EXISTS dim_location (
    location_id INT,
    state STRING,
    city STRING
)
STORED AS PARQUET
LOCATION '/data_lake/gold/ev_adoption_by_region';

-- ============================================
-- DIMENSION: TIME
-- ============================================
CREATE EXTERNAL TABLE IF NOT EXISTS dim_time (
    time_id INT,
    year INT,
    month INT
)
STORED AS PARQUET
LOCATION '/data_lake/gold/ev_adoption_trend';

-- ============================================
-- FACT TABLE
-- ============================================
CREATE EXTERNAL TABLE IF NOT EXISTS fact_ev_adoption (
    state STRING,
    city STRING,
    make STRING,
    model STRING,
    year INT,
    month INT,
    vehicle_count BIGINT,
    avg_electric_range DOUBLE
)
STORED AS PARQUET
LOCATION '/data_lake/gold/fact_ev_adoption';