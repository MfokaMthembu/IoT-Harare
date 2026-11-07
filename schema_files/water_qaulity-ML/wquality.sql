
BEGIN;

-- 1. Registry for water-quality sensors
CREATE TABLE IF NOT EXISTS water_quality_sensor (
    sensor_id TEXT PRIMARY KEY,
    ward_id INTEGER NOT NULL REFERENCES ward(ward_id),
    sensor_name TEXT NOT NULL,
    parameter TEXT NOT NULL CHECK (
        parameter IN (
            'ph',
            'turbidity',
            'conductivity',
            'chlorine_residual',
            'temperature'
        )
    ),
    unit TEXT NOT NULL,
    active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 2. Water-quality telemetry
CREATE TABLE IF NOT EXISTS water_quality_telemetry (
    time TIMESTAMPTZ NOT NULL,
    sensor_id TEXT NOT NULL
        REFERENCES water_quality_sensor(sensor_id),
    metric_type TEXT NOT NULL CHECK (
        metric_type IN (
            'ph',
            'turbidity',
            'conductivity',
            'chlorine_residual',
            'temperature'
        )
    ),
    value DOUBLE PRECISION NOT NULL,
    received_at TIMESTAMPTZ,
    edge_received_at TIMESTAMPTZ,
    quality_status TEXT NOT NULL DEFAULT 'valid'
        CHECK (
            quality_status IN (
                'valid',
                'suspect',
                'invalid'
            )
        ),
    UNIQUE (sensor_id, metric_type, time)
);

-- 3. Infrastructure asset register
CREATE TABLE IF NOT EXISTS asset (
    asset_id TEXT PRIMARY KEY,
    asset_name TEXT NOT NULL,
    asset_type TEXT NOT NULL CHECK (
        asset_type IN (
            'pipeline_segment',
            'valve',
            'filter',
            'pump',
            'reservoir',
            'treatment_unit'
        )
    ),
    ward_id INTEGER REFERENCES ward(ward_id),
    status TEXT NOT NULL DEFAULT 'active'
        CHECK (
            status IN (
                'active',
                'inactive',
                'maintenance',
                'fault'
            )
        ),
    description TEXT,
    metadata JSONB NOT NULL DEFAULT '{}'::JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 4. Relationships between infrastructure assets
CREATE TABLE IF NOT EXISTS asset_relationship (
    from_asset_id TEXT NOT NULL REFERENCES asset(asset_id),
    to_asset_id TEXT NOT NULL REFERENCES asset(asset_id),
    relationship_type TEXT NOT NULL CHECK (
        relationship_type IN (
            'connects_to',
            'upstream_of',
            'downstream_of',
            'isolates',
            'feeds',
            'protects'
        )
    ),
    description TEXT,
    PRIMARY KEY (
        from_asset_id,
        to_asset_id,
        relationship_type
    ),
    CHECK (from_asset_id <> to_asset_id)
);

-- 5. Configurable water-quality limits
CREATE TABLE IF NOT EXISTS water_quality_limits (
    limit_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    metric_type TEXT NOT NULL CHECK (
        metric_type IN (
            'ph',
            'turbidity',
            'conductivity',
            'chlorine_residual',
            'temperature'
        )
    ),
    ward_id INTEGER REFERENCES ward(ward_id),
    asset_id TEXT REFERENCES asset(asset_id),
    warning_min DOUBLE PRECISION,
    warning_max DOUBLE PRECISION,
    critical_min DOUBLE PRECISION,
    critical_max DOUBLE PRECISION,
    unit TEXT NOT NULL,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    description TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (warning_min IS NULL OR warning_max IS NULL
           OR warning_min <= warning_max),
    CHECK (critical_min IS NULL OR critical_max IS NULL
           OR critical_min <= critical_max),
    CHECK (
        ward_id IS NOT NULL
        OR asset_id IS NOT NULL
        OR (ward_id IS NULL AND asset_id IS NULL)
    )
);

-- 6. Decision-support events
CREATE TABLE IF NOT EXISTS decision_event (
    event_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    detected_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    source TEXT NOT NULL CHECK (
        source IN ('threshold', 'rule', 'ml', 'operator')
    ),
    event_type TEXT NOT NULL,
    severity TEXT NOT NULL CHECK (
        severity IN ('info', 'warning', 'critical')
    ),
    status TEXT NOT NULL DEFAULT 'open' CHECK (
        status IN (
            'open',
            'acknowledged',
            'resolved',
            'false_positive'
        )
    ),
    sensor_id TEXT REFERENCES water_quality_sensor(sensor_id),
    asset_id TEXT REFERENCES asset(asset_id),
    metric_type TEXT,
    observed_value DOUBLE PRECISION,
    anomaly_score DOUBLE PRECISION,
    message TEXT NOT NULL,
    recommended_action TEXT,
    details JSONB NOT NULL DEFAULT '{}'::JSONB
);

-- 7. Action and recommendation history
CREATE TABLE IF NOT EXISTS action_log (
    action_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_id BIGINT NOT NULL REFERENCES decision_event(event_id),
    asset_id TEXT REFERENCES asset(asset_id),
    action_type TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'proposed' CHECK (
        status IN (
            'proposed',
            'approved',
            'simulated',
            'completed',
            'rejected',
            'failed'
        )
    ),
    requested_by TEXT,
    notes TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ
);

-- Useful query indexes
CREATE INDEX IF NOT EXISTS wq_telemetry_sensor_time_idx
    ON water_quality_telemetry (sensor_id, time DESC);

CREATE INDEX IF NOT EXISTS wq_telemetry_metric_time_idx
    ON water_quality_telemetry (metric_type, time DESC);

CREATE INDEX IF NOT EXISTS asset_type_status_idx
    ON asset (asset_type, status);

CREATE INDEX IF NOT EXISTS decision_event_time_idx
    ON decision_event (detected_at DESC);

CREATE INDEX IF NOT EXISTS decision_event_status_severity_idx
    ON decision_event (status, severity);

CREATE INDEX IF NOT EXISTS action_log_event_time_idx
    ON action_log (event_id, created_at DESC);

COMMIT;
