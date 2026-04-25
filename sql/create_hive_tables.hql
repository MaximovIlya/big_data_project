-- Hive DDL for SF Police Incident Reports
-- Stage II: Data Storage & Preparation

CREATE DATABASE IF NOT EXISTS sf_incidents_db;

USE sf_incidents_db;

-- External table pointing to Sqoop output in HDFS
DROP TABLE IF EXISTS incidents_raw;

CREATE EXTERNAL TABLE incidents_raw (
    row_id                  BIGINT,
    incident_datetime       STRING,
    incident_date           STRING,
    incident_time           STRING,
    incident_year           INT,
    incident_day_of_week    STRING,
    report_datetime         STRING,
    incident_id             BIGINT,
    incident_number         BIGINT,
    cad_number              BIGINT,
    report_type_code        STRING,
    report_type_description STRING,
    filed_online            STRING,
    incident_code           INT,
    incident_category       STRING,
    incident_subcategory    STRING,
    incident_description    STRING,
    resolution              STRING,
    intersection            STRING,
    cnn                     BIGINT,
    police_district         STRING,
    analysis_neighborhood   STRING,
    supervisor_district     INT,
    supervisor_district_2012 INT,
    latitude                DOUBLE,
    longitude               DOUBLE,
    point                   STRING
)
STORED AS PARQUET
LOCATION '${hdfsloc}';


-- Optimized PARQUET table with Snappy compression
DROP TABLE IF EXISTS incidents_parquet;

SET hive.exec.compress.output=true;
SET parquet.compression=SNAPPY;

CREATE TABLE incidents_parquet
STORED AS PARQUET
TBLPROPERTIES ("parquet.compression"="SNAPPY")
AS
SELECT
    row_id,
    CAST(FROM_UNIXTIME(CAST(incident_datetime AS BIGINT) DIV 1000) AS TIMESTAMP) AS incident_datetime,
    CAST(FROM_UNIXTIME(CAST(incident_date AS BIGINT) DIV 1000) AS DATE)          AS incident_date,
    incident_time,
    incident_year,
    incident_day_of_week,
    CAST(FROM_UNIXTIME(CAST(report_datetime AS BIGINT) DIV 1000) AS TIMESTAMP)  AS report_datetime,
    incident_id,
    incident_number,
    cad_number,
    report_type_code,
    report_type_description,
    CASE WHEN lower(filed_online) = 'true' THEN TRUE ELSE FALSE END AS filed_online,
    incident_code,
    incident_category,
    incident_subcategory,
    incident_description,
    resolution,
    intersection,
    cnn,
    police_district,
    analysis_neighborhood,
    supervisor_district,
    supervisor_district_2012,
    latitude,
    longitude
FROM incidents_raw
WHERE incident_category IS NOT NULL
  AND latitude IS NOT NULL
  AND longitude IS NOT NULL;


-- Summary stats view
DROP VIEW IF EXISTS v_incident_summary;
CREATE VIEW v_incident_summary AS
SELECT
    incident_category,
    COUNT(*)                    AS total_incidents,
    COUNT(DISTINCT police_district) AS districts_affected,
    MIN(incident_datetime)      AS first_seen,
    MAX(incident_datetime)      AS last_seen,
    AVG(latitude)               AS avg_lat,
    AVG(longitude)              AS avg_lon
FROM incidents_parquet
GROUP BY incident_category;
