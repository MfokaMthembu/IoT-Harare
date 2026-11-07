
-- Edge (Node-RED) migration for the HRE telemetry database

-- Add received_at column to telemetry and alert 
-- time        = when the reading/fault happened (stamped at the edge)
-- received_at = when it reached this database
-- Backfilled data after an outage has received_at much later than time.
-- NULL means "delivered live / seeded" and is treated as on time.
ALTER TABLE telemetry ADD COLUMN IF NOT EXISTS received_at timestamptz;
ALTER TABLE alert     ADD COLUMN IF NOT EXISTS received_at timestamptz;

-- How the delays are defined
--   transport  = edge_received_at - time          sensor timestamp -> arrival at the edge (MQTT hop)
--   ingestion  = received_at - edge_received_at   edge validation + batching + database insert
--   pipeline   = received_at - time               sensor -> database
--   detection  = first alert received_at - fault_start
-- Clock note: time and edge_received_at come from the VM clock, received_at from the database server's clock,
-- so ingestion includes any clock difference between them (keep both on NTP). Sensor timestamps have 1 s resolution.
-- =====================================================================

ALTER TABLE telemetry ADD COLUMN IF NOT EXISTS edge_received_at timestamptz;
ALTER TABLE alert     ADD COLUMN IF NOT EXISTS edge_received_at timestamptz;

-- replayed batch can never create duplicates
CREATE UNIQUE INDEX IF NOT EXISTS telemetry_dedupe
  ON telemetry (sensor_id, metric_type, time);
-- NULLS NOT DISTINCT (PG15+) so watchdog alerts (metric_type NULL) dedupe too
CREATE UNIQUE INDEX IF NOT EXISTS alert_dedupe
  ON alert (sensor_id, fault_type, severity, metric_type, time) NULLS NOT DISTINCT;

-- Edge event log: uplink and power transitions, sync summaries --
CREATE TABLE IF NOT EXISTS edge_event (
  event_id    bigserial PRIMARY KEY,
  time        timestamptz NOT NULL,
  edge_id     text        NOT NULL,
  event_type  text        NOT NULL,  -- uplink_down | uplink_up | power_loss | power_restored
                                     -- sync_completed | ward_outage_suspected | ward_restored
  ward_id     int,
  detail      jsonb,
  received_at timestamptz,
  UNIQUE (edge_id, event_type, time)
);
CREATE INDEX IF NOT EXISTS edge_event_time_idx ON edge_event (time DESC);

-- =====================================================================
-- latency instrumentation
--   * edge_received_at: when the Node-RED edge received the reading/alert (splits transport from ingestion)
--   * fault_label:      ground truth from the sensor fleet (which fault, on which sensor, from when)
-- Run once after edge-migration-2.sql. Safe to re-run.
--   psql "<url>" -v ON_ERROR_STOP=1 -f edge-migration-3.sql
--
-- How the delays are defined
--   transport  = edge_received_at - time          sensor timestamp -> arrival at the edge (MQTT hop)
--   ingestion  = received_at - edge_received_at   edge validation + batching + database insert
--   pipeline   = received_at - time               sensor -> database
--   detection  = first alert received_at - fault_start
-- Clock note: time and edge_received_at come from the VM clock, received_at from the database server's clock,
-- so ingestion includes any clock difference between them (keep both on NTP). Sensor timestamps have 1 s resolution.
-- =====================================================================

CREATE TABLE IF NOT EXISTS fault_label (
  label_id    bigserial PRIMARY KEY,
  time        timestamptz NOT NULL,                         -- when this label event happened
  event       text NOT NULL CHECK (event IN ('fault_start', 'fault_end')),
  sensor_id   text NOT NULL REFERENCES sensor,
  kind        text NOT NULL,                                -- burst, leak, blockage, ...
  ramp_s      double precision,  CHECK (fault_until IS NULL OR fault_until >= fault_start)                    -- 0 = sudden, otherwise seconds to full effect
  fault_start timestamptz NOT NULL,
  fault_until timestamptz,
  received_at timestamptz,
  UNIQUE (sensor_id, event, fault_start)                    -- a replayed label can never duplicate
);
CREATE INDEX IF NOT EXISTS fault_label_start_idx ON fault_label (fault_start DESC);

-- It can read reference tables and INSERT telemetry/alerts/events.
SELECT format('CREATE ROLE hre_ingest LOGIN PASSWORD %L', :'ingest_pw')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'hre_ingest')
\gexec

SELECT format('ALTER ROLE hre_ingest PASSWORD %L', :'ingest_pw')
\gexec

SELECT format('GRANT CONNECT ON DATABASE %I TO hre_ingest', current_database())
\gexec

GRANT USAGE ON SCHEMA public TO hre_ingest;
GRANT SELECT ON sensor, ward, municipality, baseline, fault_label TO hre_ingest;
GRANT INSERT ON telemetry, alert, edge_event, fault_label TO hre_ingest;
GRANT USAGE, SELECT ON SEQUENCE alert_alert_id_seq, edge_event_event_id_seq, fault_label_label_id_seq TO hre_ingest;

-- Grafana role must be able to read the new table
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'hre_grafana') THEN
    GRANT SELECT ON edge_event, fault_label TO hre_grafana;
  END IF;
END $$;

-- 5. Late-arriving data must reach the continuous aggregate --------------
-- The old policy only looked back 2 hours, so rows backfilled after a longer
-- outage were never materialised. Only invalidated buckets are recomputed,
-- Repeat for your 1-hour cagg if you made one.
SELECT remove_continuous_aggregate_policy('telemetry_5m', if_exists => true);
SELECT add_continuous_aggregate_policy('telemetry_5m',
  start_offset      => INTERVAL '7 days',
  end_offset        => INTERVAL '5 minutes',
  schedule_interval => INTERVAL '5 minutes');
