CREATE DATABASE IF NOT EXISTS idebench_{size};
CREATE TABLE IF NOT EXISTS idebench_{size}.tbl_flights
(
    YEAR_DATE Int32,
    UNIQUE_CARRIER String,
    ORIGIN String,
    ORIGIN_STATE_ABR String,
    DEST String,
    DEST_STATE_ABR String,
    DEP_DELAY Float64,
    TAXI_OUT Float64,
    TAXI_IN Float64,
    ARR_DELAY Float64,
    AIR_TIME Float64,
    DISTANCE Float64
)
ENGINE = MergeTree ORDER BY tuple();
