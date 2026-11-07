-- HRE evaluation queries. Replace the two timestamps with PHASE1_START and PHASE1_END from times.txt (UTC).
\set t0 '''2026-11-06T16:11:30Z'''
\set t1 '''2026-11-06T17:14:58Z'''

-- Q1  Table 4.7 reading stages (live rows only, one row per report via flow_rate)
SELECT stage, count(*) AS n,
       round(percentile_cont(0.50) WITHIN GROUP (ORDER BY s)::numeric, 3) AS p50,
       round(percentile_cont(0.95) WITHIN GROUP (ORDER BY s)::numeric, 3) AS p95,
       round(max(s)::numeric, 3) AS max
FROM (
  SELECT 'transport (time -> edge)'  AS stage, extract(epoch FROM edge_received_at - time) AS s FROM telemetry
   WHERE metric_type = 'flow_rate' AND time BETWEEN :t0 AND :t1 AND received_at - time < interval '30 seconds'
  UNION ALL
  SELECT 'ingestion (edge -> db)', extract(epoch FROM received_at - edge_received_at) FROM telemetry
   WHERE metric_type = 'flow_rate' AND time BETWEEN :t0 AND :t1 AND received_at - time < interval '30 seconds'
  UNION ALL
  SELECT 'pipeline (time -> db)', extract(epoch FROM received_at - time) FROM telemetry
   WHERE metric_type = 'flow_rate' AND time BETWEEN :t0 AND :t1 AND received_at - time < interval '30 seconds'
) x GROUP BY stage ORDER BY stage;

-- Q2  Alerts raised during Phase 1, by sensor and type (shows hits, wrong types and extras)
SELECT sensor_id, fault_type, severity, source, count(*) AS n
FROM alert
WHERE received_at BETWEEN :t0 AND :t1 AND severity <> 'cleared'
GROUP BY 1, 2, 3, 4 ORDER BY 1, 2;

-- Q3  False alarms: non-cleared alerts on sensors that were NOT under test in Phase 1
SELECT fault_type, severity, source, count(*) AS n
FROM alert
WHERE received_at BETWEEN :t0 AND :t1 AND severity <> 'cleared'
  AND sensor_id NOT IN ('harare-water-03','chitungwiza-water-03','epworth-water-03','ruwa-sewer-03',
                        'harare-sewer-07','chitungwiza-water-08','epworth-water-05','ruwa-water-08',
                        'harare-sewer-12','chitungwiza-sewer-12')
GROUP BY 1, 2, 3 ORDER BY n DESC;

-- Q4  Volume handled during Phase 1 (context for the results)
SELECT count(*) AS readings, count(DISTINCT sensor_id) AS sensors
FROM telemetry WHERE time BETWEEN :t0 AND :t1;
