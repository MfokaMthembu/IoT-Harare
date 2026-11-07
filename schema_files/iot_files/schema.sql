CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TYPE metric_type_enum AS ENUM (
    'pressure',
    'flow_rate',
    'level'
);

CREATE TABLE province (
  province_id   SERIAL PRIMARY KEY,
  province_name TEXT NOT NULL,
  slug          TEXT NOT NULL);

CREATE TABLE municipality (
  municipality_id   SERIAL PRIMARY KEY,
  province_id       INT NOT NULL REFERENCES province,
  municipality_name TEXT NOT NULL,
  slug              TEXT NOT NULL,
  UNIQUE (province_id, slug));

CREATE TABLE ward (
  ward_id         SERIAL PRIMARY KEY,
  municipality_id INT NOT NULL REFERENCES municipality,
  ward_number       INT NOT NULL,
  lat DOUBLE PRECISION, lon DOUBLE PRECISION,
  UNIQUE (municipality_id, ward_id));

CREATE TABLE sensor (
  sensor_id   TEXT PRIMARY KEY,                 -- e.g. harare-water-01
  ward_id     INT NOT NULL REFERENCES ward,
  sensor_type TEXT NOT NULL CHECK (sensor_type IN ('water','sewer')),
  -- asset_info  JSONB DEFAULT '{}',               -- model, install date, pipe diameter...
  active      BOOLEAN DEFAULT true);

CREATE TABLE baseline (
  sensor_type TEXT NOT NULL,
  metric_type metric_type_enum NOT NULL,
  unit        TEXT,
  normal_min  DOUBLE PRECISION,
  normal_max  DOUBLE PRECISION,
  crit_min    DOUBLE PRECISION,
  crit_max    DOUBLE PRECISION,
  PRIMARY KEY (sensor_type, metric_type)
);

CREATE TABLE telemetry (
  time        TIMESTAMPTZ NOT NULL,
  sensor_id   TEXT NOT NULL REFERENCES sensor,
  metric_type metric_type_enum NOT NULL,
  value       DOUBLE PRECISION NOT NULL);
SELECT create_hypertable('telemetry','time');
CREATE INDEX ON telemetry (sensor_id, metric_type, time DESC);

CREATE TABLE alert (
  alert_id    BIGSERIAL PRIMARY KEY,
  time        TIMESTAMPTZ NOT NULL DEFAULT now(),
  sensor_id   TEXT NOT NULL REFERENCES sensor,
  source      TEXT NOT NULL,        -- baseline | sensor | watchdog
  fault_type  TEXT NOT NULL,        -- leak, burst, blockage, offline, out_of_range...
  severity    TEXT NOT NULL,        -- warning | critical | cleared
  metric_type metric_type_enum, value DOUBLE PRECISION,
  message     TEXT);
CREATE INDEX ON alert (time DESC);

-- 1-minute aggregation
CREATE MATERIALIZED VIEW telemetry_1m WITH (timescaledb.continuous) AS
SELECT time_bucket('1 minute', time) AS bucket, sensor_id, metric_type,
       avg(value) avg, min(value) min, max(value) max
FROM telemetry GROUP BY 1,2,3 WITH NO DATA;
SELECT add_continuous_aggregate_policy('telemetry_1m',
  start_offset => INTERVAL '1 hour', end_offset => INTERVAL '1 minute',
  schedule_interval => INTERVAL '1 minute');

-- Province: Harare. 
INSERT INTO province(province_name,slug) VALUES ('HRE-METRO','hre-metro');

-- Municipality: Harare, Chitungwiza, Epworth & Ruwa
INSERT INTO municipality(province_id,municipality_name,slug) VALUES (1,'Harare','harare');
INSERT INTO municipality(province_id,municipality_name,slug) VALUES (1,'Chitungwiza','chitungwiza');
INSERT INTO municipality(province_id,municipality_name,slug) VALUES (1,'Epworth','epworth');
INSERT INTO municipality(province_id,municipality_name,slug) VALUES (1,'Ruwa','ruwa');

-- Ward: Constituencies in the city of Harare.
INSERT INTO ward(ward_id,municipality_id,ward_number,lat,lon) VALUES (1,1,1,-17.946, 31.015);
INSERT INTO ward(ward_id,municipality_id,ward_number,lat,lon) VALUES (2,1,2,-17.841, 31.075);
INSERT INTO ward(ward_id,municipality_id,ward_number,lat,lon) VALUES (3,1,3,-17.86, 31.041);
INSERT INTO ward(ward_id,municipality_id,ward_number,lat,lon) VALUES (4,1,4,-17.846, 31.042);
INSERT INTO ward(ward_id,municipality_id,ward_number,lat,lon) VALUES (5,1,5,-17.822, 31.02);
INSERT INTO ward(ward_id,municipality_id,ward_number,lat,lon) VALUES (6,1,6,-17.826, 31.05);
INSERT INTO ward(ward_id,municipality_id,ward_number,lat,lon) VALUES (7,1,7,-17.794, 31.047);
INSERT INTO ward(ward_id,municipality_id,ward_number,lat,lon) VALUES (8,1,8,-17.794, 31.101);
INSERT INTO ward(ward_id,municipality_id,ward_number,lat,lon) VALUES (9,1,9,-17.807, 31.154);
INSERT INTO ward(ward_id,municipality_id,ward_number,lat,lon) VALUES (10,1,10,-17.862, 31.052);
INSERT INTO ward(ward_id,municipality_id,ward_number,lat,lon) VALUES (11,1,11,-17.858, 31.027);
INSERT INTO ward(ward_id,municipality_id,ward_number,lat,lon) VALUES (12,1,12,-17.868, 31.032);
INSERT INTO ward(ward_id,municipality_id,ward_number,lat,lon) VALUES (13,1,13,-17.864, 30.989);
INSERT INTO ward(ward_id,municipality_id,ward_number,lat,lon) VALUES (14,1,14,-17.855, 30.964);
INSERT INTO ward(ward_id,municipality_id,ward_number,lat,lon) VALUES (15,1,15,-17.83, 30.97);
INSERT INTO ward(ward_id,municipality_id,ward_number,lat,lon) VALUES (16,1,16,-17.791, 30.98);
INSERT INTO ward(ward_id,municipality_id,ward_number,lat,lon) VALUES (17,1,17,-17.756, 31.06);
INSERT INTO ward(ward_id,municipality_id,ward_number,lat,lon) VALUES (18,1,18,-17.73, 31.148);
INSERT INTO ward(ward_id,municipality_id,ward_number,lat,lon) VALUES (19,1,19,-17.83, 31.192);
INSERT INTO ward(ward_id,municipality_id,ward_number,lat,lon) VALUES (20,1,20,-17.842, 31.209);
INSERT INTO ward(ward_id,municipality_id,ward_number,lat,lon) VALUES (21,1,21,-17.858, 31.174);
INSERT INTO ward(ward_id,municipality_id,ward_number,lat,lon) VALUES (22,1,22,-17.874, 31.091);
INSERT INTO ward(ward_id,municipality_id,ward_number,lat,lon) VALUES (23,1,23,-17.902, 31.025);
INSERT INTO ward(ward_id,municipality_id,ward_number,lat,lon) VALUES (24,1,24,-17.889, 31);
INSERT INTO ward(ward_id,municipality_id,ward_number,lat,lon) VALUES (25,1,25,-17.884, 30.99);
INSERT INTO ward(ward_id,municipality_id,ward_number,lat,lon) VALUES (26,1,26,-17.887, 30.971);
INSERT INTO ward(ward_id,municipality_id,ward_number,lat,lon) VALUES (27,1,27,-17.91, 30.981);
INSERT INTO ward(ward_id,municipality_id,ward_number,lat,lon) VALUES (28,1,28,-17.916, 30.961);
INSERT INTO ward(ward_id,municipality_id,ward_number,lat,lon) VALUES (29,1,29,-17.9, 30.971);
INSERT INTO ward(ward_id,municipality_id,ward_number,lat,lon) VALUES (30,1,30,-17.896, 30.954);
INSERT INTO ward(ward_id,municipality_id,ward_number,lat,lon) VALUES (31,1,31,-17.913, 30.95);
INSERT INTO ward(ward_id,municipality_id,ward_number,lat,lon) VALUES (32,1,32,-17.906, 30.939);
INSERT INTO ward(ward_id,municipality_id,ward_number,lat,lon) VALUES (33,1,33,-17.89, 30.938);
INSERT INTO ward(ward_id,municipality_id,ward_number,lat,lon) VALUES (34,1,34,-17.859, 30.909);
INSERT INTO ward(ward_id,municipality_id,ward_number,lat,lon) VALUES (35,1,35,-17.869, 30.94);
INSERT INTO ward(ward_id,municipality_id,ward_number,lat,lon) VALUES (36,1,36,-17.859, 30.936);
INSERT INTO ward(ward_id,municipality_id,ward_number,lat,lon) VALUES (37,1,37,-17.84, 30.921);
INSERT INTO ward(ward_id,municipality_id,ward_number,lat,lon) VALUES (38,1,38,-17.828, 30.916);
INSERT INTO ward(ward_id,municipality_id,ward_number,lat,lon) VALUES (39,1,39,-17.809, 30.925);
INSERT INTO ward(ward_id,municipality_id,ward_number,lat,lon) VALUES (40,1,40,-17.798, 30.916);
INSERT INTO ward(ward_id,municipality_id,ward_number,lat,lon) VALUES (41,1,41,-17.749, 30.989);
INSERT INTO ward(ward_id,municipality_id,ward_number,lat,lon) VALUES (42,1,42,-17.698, 31.095);
INSERT INTO ward(ward_id,municipality_id,ward_number,lat,lon) VALUES (43,1,43,-17.883, 30.918);
INSERT INTO ward(ward_id,municipality_id,ward_number,lat,lon) VALUES (44,1,44,-17.833, 30.899);
INSERT INTO ward(ward_id,municipality_id,ward_number,lat,lon) VALUES (45,1,45,-17.805, 30.941);


-- Ward: Constituencies in Chitungwiza.
INSERT INTO ward(ward_id,municipality_id,ward_number,lat,lon) VALUES (46,2,1,-17.986, 31.054);
INSERT INTO ward(ward_id,municipality_id,ward_number,lat,lon) VALUES (47,2,2,-17.992, 31.049);
INSERT INTO ward(ward_id,municipality_id,ward_number,lat,lon) VALUES (48,2,3,-17.989, 31.041);
INSERT INTO ward(ward_id,municipality_id,ward_number,lat,lon) VALUES (49,2,4,-17.994, 31.031);
INSERT INTO ward(ward_id,municipality_id,ward_number,lat,lon) VALUES (50,2,5,-17.998, 31.033);
INSERT INTO ward(ward_id,municipality_id,ward_number,lat,lon) VALUES (51,2,6,-18.007, 31.027);
INSERT INTO ward(ward_id,municipality_id,ward_number,lat,lon) VALUES (52,2,7,-17.999, 31.052);
INSERT INTO ward(ward_id,municipality_id,ward_number,lat,lon) VALUES (53,2,8,-17.993, 31.055);
INSERT INTO ward(ward_id,municipality_id,ward_number,lat,lon) VALUES (54,2,9,-18.004, 31.057);
INSERT INTO ward(ward_id,municipality_id,ward_number,lat,lon) VALUES (55,2,10,-17.996, 31.062);
INSERT INTO ward(ward_id,municipality_id,ward_number,lat,lon) VALUES (56,2,11,-18.01, 31.055);
INSERT INTO ward(ward_id,municipality_id,ward_number,lat,lon) VALUES (57,2,12,-18.023, 31.046);
INSERT INTO ward(ward_id,municipality_id,ward_number,lat,lon) VALUES (58,2,13,-18.016, 31.065);
INSERT INTO ward(ward_id,municipality_id,ward_number,lat,lon) VALUES (59,2,14,-18.005, 31.068);
INSERT INTO ward(ward_id,municipality_id,ward_number,lat,lon) VALUES (60,2,15,-18.011, 31.079);
INSERT INTO ward(ward_id,municipality_id,ward_number,lat,lon) VALUES (61,2,16,-18.022, 31.073);
INSERT INTO ward(ward_id,municipality_id,ward_number,lat,lon) VALUES (62,2,17,-18.027, 31.078);
INSERT INTO ward(ward_id,municipality_id,ward_number,lat,lon) VALUES (63,2,18,-18.04, 31.066);
INSERT INTO ward(ward_id,municipality_id,ward_number,lat,lon) VALUES (64,2,19,-18.02, 31.095);
INSERT INTO ward(ward_id,municipality_id,ward_number,lat,lon) VALUES (65,2,20,-18.007, 31.087);
INSERT INTO ward(ward_id,municipality_id,ward_number,lat,lon) VALUES (66,2,21,-18.01, 31.096);
INSERT INTO ward(ward_id,municipality_id,ward_number,lat,lon) VALUES (67,2,22,-18.027, 31.114);
INSERT INTO ward(ward_id,municipality_id,ward_number,lat,lon) VALUES (68,2,23,-18.027, 31.099);
INSERT INTO ward(ward_id,municipality_id,ward_number,lat,lon) VALUES (69,2,24,-18.032, 31.085);
INSERT INTO ward(ward_id,municipality_id,ward_number,lat,lon) VALUES (70,2,25,-18.017, 31.11);

-- Ward: Constituencies in Epworth.
INSERT INTO ward(ward_id,municipality_id,ward_number,lat,lon) VALUES (71,3,1,-17.877, 31.134);
INSERT INTO ward(ward_id,municipality_id,ward_number,lat,lon) VALUES (72,3,2,-17.896, 31.14);
INSERT INTO ward(ward_id,municipality_id,ward_number,lat,lon) VALUES (73,3,3,-17.916, 31.146);
INSERT INTO ward(ward_id,municipality_id,ward_number,lat,lon) VALUES (74,3,4,-17.909, 31.164);
INSERT INTO ward(ward_id,municipality_id,ward_number,lat,lon) VALUES (75,3,5,-17.898, 31.153);
INSERT INTO ward(ward_id,municipality_id,ward_number,lat,lon) VALUES (76,3,6,-17.885, 31.179);
INSERT INTO ward(ward_id,municipality_id,ward_number,lat,lon) VALUES (77,3,7,-17.882, 31.15);

-- Ward: Constituencies in Ruwa.
INSERT INTO ward(ward_id,municipality_id,ward_number,lat,lon) VALUES (78,4,1,-17.893, 31.249);
INSERT INTO ward(ward_id,municipality_id,ward_number,lat,lon) VALUES (79,4,2,-17.905, 31.261);
INSERT INTO ward(ward_id,municipality_id,ward_number,lat,lon) VALUES (80,4,3,-17.897, 31.241);
INSERT INTO ward(ward_id,municipality_id,ward_number,lat,lon) VALUES (81,4,4,-17.901, 31.236);
INSERT INTO ward(ward_id,municipality_id,ward_number,lat,lon) VALUES (82,4,5,-17.912, 31.233);
INSERT INTO ward(ward_id,municipality_id,ward_number,lat,lon) VALUES (83,4,6,-17.906, 31.232);
INSERT INTO ward(ward_id,municipality_id,ward_number,lat,lon) VALUES (84,4,7,-17.898, 31.216);
INSERT INTO ward(ward_id,municipality_id,ward_number,lat,lon) VALUES (85,4,8,-17.865, 31.212);
INSERT INTO ward(ward_id,municipality_id,ward_number,lat,lon) VALUES (86,4,9,-17.871, 31.24);
--- Ward: Constituencies in Ruwa - ward 10-18 locationdata was not available hence the use of estimated coordinates from around Ruwa.
INSERT INTO ward(ward_id,municipality_id,ward_number,lat,lon) VALUES (87,4,10,-17.88, 31.226);
INSERT INTO ward(ward_id,municipality_id,ward_number,lat,lon) VALUES (88,4,11,-17.89, 31.23);
INSERT INTO ward(ward_id,municipality_id,ward_number,lat,lon) VALUES (89,4,12,-17.90, 31.235);
INSERT INTO ward(ward_id,municipality_id,ward_number,lat,lon) VALUES (90,4,13,-17.91, 31.24);
INSERT INTO ward(ward_id,municipality_id,ward_number,lat,lon) VALUES (91,4,14,-17.895, 31.22);
INSERT INTO ward(ward_id,municipality_id,ward_number,lat,lon) VALUES (92,4,15,-17.888, 31.215);
INSERT INTO ward(ward_id,municipality_id,ward_number,lat,lon) VALUES (93,4,16,-17.882, 31.208);
INSERT INTO ward(ward_id,municipality_id,ward_number,lat,lon) VALUES (94,4,17,-17.875, 31.205);
INSERT INTO ward(ward_id,municipality_id,ward_number,lat,lon) VALUES (95,4,18,-17.868, 31.202);


INSERT INTO sensor(sensor_id,ward_id,sensor_type) VALUES
 ('harare-water-01',1,'water'),
 ('harare-sewer-01',1,'sewer'),
 ('harare-water-02',2,'water'),
 ('harare-sewer-02',2,'sewer'),
 ('harare-water-03',3,'water'),
 ('harare-sewer-03',3,'sewer'),
 ('harare-water-04',4,'water'),
 ('harare-sewer-04',4,'sewer'),
 ('harare-water-05',5,'water'),
 ('harare-sewer-05',5,'sewer'),
 ('harare-water-06',6,'water'),
 ('harare-sewer-06',6,'sewer'),
 ('harare-water-07',7,'water'),
 ('harare-sewer-07',7,'sewer'),
  ('harare-water-08',8,'water'),
  ('harare-sewer-08',8,'sewer'),
  ('harare-water-09',9,'water'),
  ('harare-sewer-09',9,'sewer'),
  ('harare-water-10',10,'water'),
  ('harare-sewer-10',10,'sewer'),
  ('harare-water-11',11,'water'),
  ('harare-sewer-11',11,'sewer'),
  ('harare-water-12',12,'water'),
  ('harare-sewer-12',12,'sewer'),
  ('harare-water-13',13,'water'),
  ('harare-sewer-13',13,'sewer'),
  ('harare-water-14',14,'water'),
  ('harare-sewer-14',14,'sewer'),
  ('harare-water-15',15,'water'),
  ('harare-sewer-15',15,'sewer'),
  ('harare-water-16',16,'water'),
  ('harare-sewer-16',16,'sewer'),
  ('harare-water-17',17,'water'),
  ('harare-sewer-17',17,'sewer'),
  ('harare-water-18',18,'water'),
  ('harare-sewer-18',18,'sewer'),
  ('harare-water-19',19,'water'),
  ('harare-sewer-19',19,'sewer'),
  ('harare-water-20',20,'water'),
  ('harare-sewer-20',20,'sewer'),
  ('harare-water-21',21,'water'),
  ('harare-sewer-21',21,'sewer'),
  ('harare-water-22',22,'water'),
  ('harare-sewer-22',22,'sewer'),
  ('harare-water-23',23,'water'),
  ('harare-sewer-23',23,'sewer'),
  ('harare-water-24',24,'water'),
  ('harare-sewer-24',24,'sewer'),
  ('harare-water-25',25,'water'),
  ('harare-sewer-25',25,'sewer'),
  ('harare-water-26',26,'water'),
  ('harare-sewer-26',26,'sewer'),
  ('harare-water-27',27,'water'),
  ('harare-sewer-27',27,'sewer'),
  ('harare-water-28',28,'water'),
  ('harare-sewer-28',28,'sewer'),
  ('harare-water-29',29,'water'),
  ('harare-sewer-29',29,'sewer'),
  ('harare-water-30',30,'water'),
  ('harare-sewer-30',30,'sewer'),
  ('harare-water-31',31,'water'),
  ('harare-sewer-31',31,'sewer'),
  ('harare-water-32',32,'water'),
  ('harare-sewer-32',32,'sewer'),
  ('harare-water-33',33,'water'),
  ('harare-sewer-33',33,'sewer'),
  ('harare-water-34',34,'water'),
  ('harare-sewer-34',34,'sewer'),
  ('harare-water-35',35,'water'),
  ('harare-sewer-35',35,'sewer'),
  ('harare-water-36',36,'water'),
  ('harare-sewer-36',36,'sewer'),
  ('harare-water-37',37,'water'),
  ('harare-sewer-37',37,'sewer'),
  ('harare-water-38',38,'water'),
  ('harare-sewer-38',38,'sewer'),
  ('harare-water-39',39,'water'),
  ('harare-sewer-39',39,'sewer'),
  ('harare-water-40',40,'water'),
  ('harare-sewer-40',40,'sewer'),
  ('harare-water-41',41,'water'),
  ('harare-sewer-41',41,'sewer'),
  ('harare-water-42',42,'water'),
  ('harare-sewer-42',42,'sewer'),
  ('harare-water-43',43,'water'),
  ('harare-sewer-43',43,'sewer'),
  ('harare-water-44',44,'water'),
  ('harare-sewer-44',44,'sewer'),
  ('harare-water-45',45,'water'),
  ('harare-sewer-45',45,'sewer');

 -- Chitungwiza water and sewer sensors
INSERT INTO sensor(sensor_id,ward_id,sensor_type) VALUES
 ('chitungwiza-water-01',46,'water'),
 ('chitungwiza-sewer-01',46,'sewer'),
 ('chitungwiza-water-02',47,'water'),
 ('chitungwiza-sewer-02',47,'sewer'),
 ('chitungwiza-water-03',48,'water'),
 ('chitungwiza-sewer-03',48,'sewer'),
 ('chitungwiza-water-04',49,'water'),
 ('chitungwiza-sewer-04',49,'sewer'),
 ('chitungwiza-water-05',50,'water'),
 ('chitungwiza-sewer-05',50,'sewer'),
  ('chitungwiza-water-06',51,'water'),
 ('chitungwiza-sewer-06',51,'sewer'),
  ('chitungwiza-water-07',52,'water'),
 ('chitungwiza-sewer-07',52,'sewer'),
  ('chitungwiza-water-08',53,'water'),
 ('chitungwiza-sewer-08',53,'sewer'),
  ('chitungwiza-water-09',54,'water'),
 ('chitungwiza-sewer-09',54,'sewer'),
  ('chitungwiza-water-10',55,'water'),
 ('chitungwiza-sewer-10',55,'sewer'),
  ('chitungwiza-water-11',56,'water'),
 ('chitungwiza-sewer-11',56,'sewer'),
  ('chitungwiza-water-12',57,'water'),
 ('chitungwiza-sewer-12',57,'sewer'),
  ('chitungwiza-water-13',58,'water'),
 ('chitungwiza-sewer-13',58,'sewer'),
  ('chitungwiza-water-14',59,'water'),
 ('chitungwiza-sewer-14',59,'sewer'),
  ('chitungwiza-water-15',60,'water'),
 ('chitungwiza-sewer-15',60,'sewer'),
  ('chitungwiza-water-16',61,'water'),
 ('chitungwiza-sewer-16',61,'sewer'),
  ('chitungwiza-water-17',62,'water'),
 ('chitungwiza-sewer-17',62,'sewer'),
  ('chitungwiza-water-18',63,'water'),
 ('chitungwiza-sewer-18',63,'sewer'),
  ('chitungwiza-water-19',64,'water'),
 ('chitungwiza-sewer-19',64,'sewer'),
  ('chitungwiza-water-20',65,'water'),
 ('chitungwiza-sewer-20',65,'sewer'),
  ('chitungwiza-water-21',66,'water'),
 ('chitungwiza-sewer-21',66,'sewer'),
  ('chitungwiza-water-22',67,'water'),
 ('chitungwiza-sewer-22',67,'sewer'),
  ('chitungwiza-water-23',68,'water'),
 ('chitungwiza-sewer-23',68,'sewer'),
  ('chitungwiza-water-24',69,'water'),
 ('chitungwiza-sewer-24',69,'sewer'),
  ('chitungwiza-water-25',70,'water'),
 ('chitungwiza-sewer-25',70,'sewer');
 

 -- Epworth water and sewer sensors
INSERT INTO sensor(sensor_id,ward_id,sensor_type) VALUES
 ('epworth-water-01',71,'water'),
 ('epworth-sewer-01',71,'sewer'),
 ('epworth-water-02',72,'water'),
 ('epworth-sewer-02',72,'sewer'),
 ('epworth-water-03',73,'water'),
 ('epworth-sewer-03',73,'sewer'),
 ('epworth-water-04',74,'water'),
 ('epworth-sewer-04',74,'sewer'),
 ('epworth-water-05',75,'water'),
 ('epworth-sewer-05',75,'sewer'),
 ('epworth-water-06',76,'water'),
 ('epworth-sewer-06',76,'sewer'),
 ('epworth-water-07',77,'water'),
 ('epworth-sewer-07',77,'sewer');

 -- Ruwa water and sewer sensors
INSERT INTO sensor(sensor_id,ward_id,sensor_type) VALUES
    ('ruwa-water-01',78,'water'),
    ('ruwa-sewer-01',78,'sewer'),
    ('ruwa-water-02',79,'water'),
    ('ruwa-sewer-02',79,'sewer'),
    ('ruwa-water-03',80,'water'),
    ('ruwa-sewer-03',80,'sewer'),
    ('ruwa-water-04',81,'water'),
    ('ruwa-sewer-04',81,'sewer'),
    ('ruwa-water-05',82,'water'),
    ('ruwa-sewer-05',82,'sewer'),
    ('ruwa-water-06',83,'water'),
    ('ruwa-sewer-06',83,'sewer'),
    ('ruwa-water-07',84,'water'),
    ('ruwa-sewer-07',84,'sewer'),
    ('ruwa-water-08',85,'water'),
    ('ruwa-sewer-08',85,'sewer'),
    ('ruwa-water-09',86,'water'),
    ('ruwa-sewer-09',86,'sewer'),
    ('ruwa-water-10',87,'water'),
    ('ruwa-sewer-10',87,'sewer'),
    ('ruwa-water-11',88,'water'),
    ('ruwa-sewer-11',88,'sewer'),
    ('ruwa-water-12',89,'water'),
    ('ruwa-sewer-12',89,'sewer'),
    ('ruwa-water-13',90,'water'),
    ('ruwa-sewer-13',90,'sewer'),
    ('ruwa-water-14',91,'water'),
    ('ruwa-sewer-14',91,'sewer'),
    ('ruwa-water-15',92,'water'),
    ('ruwa-sewer-15',92,'sewer'),
    ('ruwa-water-16',93,'water'),
    ('ruwa-sewer-16',93,'sewer'),
    ('ruwa-water-17',94,'water'),
    ('ruwa-sewer-17',94,'sewer'),
    ('ruwa-water-18',95,'water'),
    ('ruwa-sewer-18',95,'sewer');

-- Baseline: simulated baseline values for water and sewer sensors. These values are used to determine if a sensor is operating within normal parameters or if it is experiencing a fault condition.
INSERT INTO baseline
    (sensor_type, metric_type, unit,
     normal_min, normal_max, crit_min, crit_max)
VALUES
    ('water', 'pressure', 'bar', 2.0, 4.0, 1.0, 5.5),
    ('water', 'flow_rate', 'L/s', 8.0, 16.0, 2.0, 25.0),
    ('water', 'level', '%', 40.0, 90.0, 20.0, 98.0),
    ('sewer', 'flow_rate', 'L/s', 3.0, 9.0, 0.5, 15.0),
    ('sewer', 'level', '%', 15.0, 55.0, 5.0, 85.0);