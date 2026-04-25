-- PostgreSQL DDL for SF Police Incident Reports
-- Stage I: Data Collection
-- Run after connecting to the sf_incidents database

DROP TABLE IF EXISTS sf_incidents CASCADE;

CREATE TABLE sf_incidents (
    row_id                   BIGINT           PRIMARY KEY,
    incident_datetime        TIMESTAMP,
    incident_date            DATE,
    incident_time            TIME,
    incident_year            SMALLINT         NOT NULL CHECK (incident_year >= 2018),
    incident_day_of_week     VARCHAR(10)      NOT NULL,
    report_datetime          TIMESTAMP,
    incident_id              BIGINT,
    incident_number          BIGINT,
    cad_number               BIGINT,
    report_type_code         VARCHAR(10),
    report_type_description  VARCHAR(100),
    filed_online             BOOLEAN          DEFAULT FALSE,
    incident_code            INTEGER,
    incident_category        VARCHAR(100)     NOT NULL,
    incident_subcategory     VARCHAR(150),
    incident_description     TEXT,
    resolution               VARCHAR(100),
    intersection             VARCHAR(250),
    cnn                      BIGINT,
    police_district          VARCHAR(50),
    analysis_neighborhood    VARCHAR(100),
    supervisor_district      SMALLINT,
    supervisor_district_2012 SMALLINT,
    latitude                 DOUBLE PRECISION CHECK (latitude BETWEEN 37.0 AND 38.5),
    longitude                DOUBLE PRECISION CHECK (longitude BETWEEN -123.5 AND -122.0),
    point                    VARCHAR(100)
);

CREATE INDEX idx_sf_incidents_category ON sf_incidents (incident_category);
CREATE INDEX idx_sf_incidents_datetime ON sf_incidents (incident_datetime);
CREATE INDEX idx_sf_incidents_district ON sf_incidents (police_district);
CREATE INDEX idx_sf_incidents_year     ON sf_incidents (incident_year);
CREATE INDEX idx_sf_incidents_geo      ON sf_incidents (latitude, longitude);
