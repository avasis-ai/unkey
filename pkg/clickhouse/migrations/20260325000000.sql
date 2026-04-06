-- Per-instance compute billing: raw checkpoints + flattened MV chain + MV-
-- backed daily billing aggregates.
--
-- Design:
--   - Raw table stores counter/gauge snapshots per container per second.
--     Billing math (max-min counters, memory integration) is monotone and
--     idempotent on replay.
--   - MVs fan out directly from raw (no cascade). Each MV aggregates to its
--     own bucket granularity. Avoids chained merge latency.
--   - container_usage_per_day_v1 is the lowest-grain billing source. Populated
--     by a MATERIALIZED VIEW writing to an AggregatingMergeTree table, so
--     invoice queries scan ~30 rows/container instead of 86400×30.
--   - Per-minute/hour/month/year MVs are DASHBOARD ONLY — they include
--     *_sum and sample_count columns that double-count duplicate inserts
--     (ReplacingMergeTree dedupe runs on merges, MVs fire on inserts).

-- Raw checkpoints ────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS default.instance_checkpoints_v1
(
    `node_id` LowCardinality(String),
    `workspace_id` String,
    `project_id` LowCardinality(String),
    `environment_id` LowCardinality(String),
    `resource_type` LowCardinality(String),
    `resource_id` LowCardinality(String),
    `pod_uid` String,
    `instance_id` String,
    `container_uid` String,
    `restart_count` UInt32 CODEC(T64, ZSTD(1)),
    `ts` Int64 CODEC(Delta, ZSTD(1)),
    `event_kind` LowCardinality(String),
    `cpu_usage_usec` Int64 CODEC(Delta, ZSTD(1)),
    `memory_bytes` Int64 CODEC(DoubleDelta, ZSTD(1)),
    `cpu_allocated_millicores` Int32 CODEC(DoubleDelta, ZSTD(1)),
    `memory_allocated_bytes` Int64 CODEC(DoubleDelta, ZSTD(1)),
    `disk_allocated_bytes` Int64 CODEC(DoubleDelta, ZSTD(3)),
    `disk_used_bytes` Int64 CODEC(DoubleDelta, ZSTD(1)),
    `network_egress_public_bytes` Int64 CODEC(Delta, ZSTD(1)),
    `network_egress_private_bytes` Int64 CODEC(Delta, ZSTD(1)),
    `network_ingress_public_bytes` Int64 CODEC(Delta, ZSTD(1)),
    `network_ingress_private_bytes` Int64 CODEC(Delta, ZSTD(1)),
    `region` LowCardinality(String),
    `platform` LowCardinality(String),
    INDEX idx_project project_id TYPE bloom_filter(0.01) GRANULARITY 4,
    INDEX idx_resource resource_id TYPE bloom_filter(0.01) GRANULARITY 4,
    INDEX idx_ts ts TYPE minmax GRANULARITY 1
)
ENGINE = ReplacingMergeTree
ORDER BY (workspace_id, container_uid, ts)
PARTITION BY toYYYYMMDD(fromUnixTimestamp64Milli(ts))
TTL toDateTime(fromUnixTimestamp64Milli(ts)) + INTERVAL 95 DAY DELETE
SETTINGS ttl_only_drop_parts = 1;

-- Per-minute dashboard MV ────────────────────────────────────────────────
-- Per-15-second dashboard MV: fine-grained history for short chart windows.
-- 4× the row count of per_minute per container → TTL 7 days.
CREATE TABLE IF NOT EXISTS default.instance_resources_per_15s_v1
(
    `time` DateTime,
    `workspace_id` String,
    `project_id` LowCardinality(String),
    `environment_id` LowCardinality(String),
    `resource_type` LowCardinality(String),
    `resource_id` LowCardinality(String),
    `container_uid` String,
    `instance_id` LowCardinality(String),
    `cpu_usage_usec_min` SimpleAggregateFunction(min, Int64),
    `cpu_usage_usec_max` SimpleAggregateFunction(max, Int64),
    `memory_bytes_sum` SimpleAggregateFunction(sum, Int64),
    `memory_bytes_max` SimpleAggregateFunction(max, Int64),
    `cpu_allocated_millicores_max` SimpleAggregateFunction(max, Int32),
    `memory_allocated_bytes_max` SimpleAggregateFunction(max, Int64),
    `disk_allocated_bytes_max` SimpleAggregateFunction(max, Int64),
    `disk_used_bytes_max` SimpleAggregateFunction(max, Int64),
    `network_egress_public_bytes_min` SimpleAggregateFunction(min, Int64),
    `network_egress_public_bytes_max` SimpleAggregateFunction(max, Int64),
    `network_egress_private_bytes_min` SimpleAggregateFunction(min, Int64),
    `network_egress_private_bytes_max` SimpleAggregateFunction(max, Int64),
    `network_ingress_public_bytes_min` SimpleAggregateFunction(min, Int64),
    `network_ingress_public_bytes_max` SimpleAggregateFunction(max, Int64),
    `network_ingress_private_bytes_min` SimpleAggregateFunction(min, Int64),
    `network_ingress_private_bytes_max` SimpleAggregateFunction(max, Int64),
    `sample_count` SimpleAggregateFunction(sum, UInt64)
)
ENGINE = AggregatingMergeTree()
ORDER BY (workspace_id, resource_id, container_uid, time)
PARTITION BY toYYYYMM(time)
TTL time + INTERVAL 7 DAY DELETE;

CREATE MATERIALIZED VIEW IF NOT EXISTS default.instance_resources_per_15s_mv_v1
TO default.instance_resources_per_15s_v1 AS
SELECT
    toStartOfInterval(fromUnixTimestamp64Milli(ts), INTERVAL 15 SECOND) AS time,
    workspace_id, project_id, environment_id, resource_type, resource_id, container_uid, instance_id,
    min(cpu_usage_usec) AS cpu_usage_usec_min,
    max(cpu_usage_usec) AS cpu_usage_usec_max,
    sum(memory_bytes) AS memory_bytes_sum,
    max(memory_bytes) AS memory_bytes_max,
    max(cpu_allocated_millicores) AS cpu_allocated_millicores_max,
    max(memory_allocated_bytes) AS memory_allocated_bytes_max,
    max(disk_allocated_bytes) AS disk_allocated_bytes_max,
    max(disk_used_bytes) AS disk_used_bytes_max,
    min(network_egress_public_bytes) AS network_egress_public_bytes_min,
    max(network_egress_public_bytes) AS network_egress_public_bytes_max,
    min(network_egress_private_bytes) AS network_egress_private_bytes_min,
    max(network_egress_private_bytes) AS network_egress_private_bytes_max,
    min(network_ingress_public_bytes) AS network_ingress_public_bytes_min,
    max(network_ingress_public_bytes) AS network_ingress_public_bytes_max,
    min(network_ingress_private_bytes) AS network_ingress_private_bytes_min,
    max(network_ingress_private_bytes) AS network_ingress_private_bytes_max,
    toUInt64(count()) AS sample_count
FROM default.instance_checkpoints_v1
GROUP BY time, workspace_id, project_id, environment_id, resource_type, resource_id, container_uid, instance_id;

CREATE TABLE IF NOT EXISTS default.instance_resources_per_minute_v1
(
    `time` DateTime,
    `workspace_id` String,
    `project_id` LowCardinality(String),
    `environment_id` LowCardinality(String),
    `resource_type` LowCardinality(String),
    `resource_id` LowCardinality(String),
    `container_uid` String,
    `instance_id` LowCardinality(String),
    `cpu_usage_usec_min` SimpleAggregateFunction(min, Int64),
    `cpu_usage_usec_max` SimpleAggregateFunction(max, Int64),
    `memory_bytes_sum` SimpleAggregateFunction(sum, Int64),
    `memory_bytes_max` SimpleAggregateFunction(max, Int64),
    `cpu_allocated_millicores_max` SimpleAggregateFunction(max, Int32),
    `memory_allocated_bytes_max` SimpleAggregateFunction(max, Int64),
    `disk_allocated_bytes_max` SimpleAggregateFunction(max, Int64),
    `disk_used_bytes_max` SimpleAggregateFunction(max, Int64),
    `network_egress_public_bytes_min` SimpleAggregateFunction(min, Int64),
    `network_egress_public_bytes_max` SimpleAggregateFunction(max, Int64),
    `network_egress_private_bytes_min` SimpleAggregateFunction(min, Int64),
    `network_egress_private_bytes_max` SimpleAggregateFunction(max, Int64),
    `network_ingress_public_bytes_min` SimpleAggregateFunction(min, Int64),
    `network_ingress_public_bytes_max` SimpleAggregateFunction(max, Int64),
    `network_ingress_private_bytes_min` SimpleAggregateFunction(min, Int64),
    `network_ingress_private_bytes_max` SimpleAggregateFunction(max, Int64),
    `sample_count` SimpleAggregateFunction(sum, UInt64)
)
ENGINE = AggregatingMergeTree()
ORDER BY (workspace_id, resource_id, container_uid, time)
PARTITION BY toYYYYMM(time)
TTL time + INTERVAL 30 DAY DELETE;

CREATE MATERIALIZED VIEW IF NOT EXISTS default.instance_resources_per_minute_mv_v1
TO default.instance_resources_per_minute_v1 AS
SELECT
    toStartOfMinute(fromUnixTimestamp64Milli(ts)) AS time,
    workspace_id, project_id, environment_id, resource_type, resource_id, container_uid, instance_id,
    min(cpu_usage_usec) AS cpu_usage_usec_min,
    max(cpu_usage_usec) AS cpu_usage_usec_max,
    sum(memory_bytes) AS memory_bytes_sum,
    max(memory_bytes) AS memory_bytes_max,
    max(cpu_allocated_millicores) AS cpu_allocated_millicores_max,
    max(memory_allocated_bytes) AS memory_allocated_bytes_max,
    max(disk_allocated_bytes) AS disk_allocated_bytes_max,
    max(disk_used_bytes) AS disk_used_bytes_max,
    min(network_egress_public_bytes) AS network_egress_public_bytes_min,
    max(network_egress_public_bytes) AS network_egress_public_bytes_max,
    min(network_egress_private_bytes) AS network_egress_private_bytes_min,
    max(network_egress_private_bytes) AS network_egress_private_bytes_max,
    min(network_ingress_public_bytes) AS network_ingress_public_bytes_min,
    max(network_ingress_public_bytes) AS network_ingress_public_bytes_max,
    min(network_ingress_private_bytes) AS network_ingress_private_bytes_min,
    max(network_ingress_private_bytes) AS network_ingress_private_bytes_max,
    toUInt64(count()) AS sample_count
FROM default.instance_checkpoints_v1
GROUP BY time, workspace_id, project_id, environment_id, resource_type, resource_id, container_uid, instance_id;

-- Per-hour dashboard MV (reads raw directly) ─────────────────────────────
CREATE TABLE IF NOT EXISTS default.instance_resources_per_hour_v1
(
    `time` DateTime,
    `workspace_id` String,
    `project_id` LowCardinality(String),
    `environment_id` LowCardinality(String),
    `resource_type` LowCardinality(String),
    `resource_id` LowCardinality(String),
    `container_uid` String,
    `instance_id` LowCardinality(String),
    `cpu_usage_usec_min` SimpleAggregateFunction(min, Int64),
    `cpu_usage_usec_max` SimpleAggregateFunction(max, Int64),
    `memory_bytes_max` SimpleAggregateFunction(max, Int64),
    `cpu_allocated_millicores_max` SimpleAggregateFunction(max, Int32),
    `memory_allocated_bytes_max` SimpleAggregateFunction(max, Int64),
    `disk_allocated_bytes_max` SimpleAggregateFunction(max, Int64),
    `disk_used_bytes_max` SimpleAggregateFunction(max, Int64),
    `network_egress_public_bytes_min` SimpleAggregateFunction(min, Int64),
    `network_egress_public_bytes_max` SimpleAggregateFunction(max, Int64),
    `network_egress_private_bytes_min` SimpleAggregateFunction(min, Int64),
    `network_egress_private_bytes_max` SimpleAggregateFunction(max, Int64),
    `network_ingress_public_bytes_min` SimpleAggregateFunction(min, Int64),
    `network_ingress_public_bytes_max` SimpleAggregateFunction(max, Int64),
    `network_ingress_private_bytes_min` SimpleAggregateFunction(min, Int64),
    `network_ingress_private_bytes_max` SimpleAggregateFunction(max, Int64),
    `sample_count` SimpleAggregateFunction(sum, UInt64)
)
ENGINE = AggregatingMergeTree()
ORDER BY (workspace_id, resource_id, container_uid, time)
PARTITION BY toYYYYMM(time)
TTL time + INTERVAL 90 DAY DELETE;

CREATE MATERIALIZED VIEW IF NOT EXISTS default.instance_resources_per_hour_mv_v1
TO default.instance_resources_per_hour_v1 AS
SELECT
    toStartOfHour(fromUnixTimestamp64Milli(ts)) AS time,
    workspace_id, project_id, environment_id, resource_type, resource_id, container_uid, instance_id,
    min(cpu_usage_usec) AS cpu_usage_usec_min,
    max(cpu_usage_usec) AS cpu_usage_usec_max,
    max(memory_bytes) AS memory_bytes_max,
    max(cpu_allocated_millicores) AS cpu_allocated_millicores_max,
    max(memory_allocated_bytes) AS memory_allocated_bytes_max,
    max(disk_allocated_bytes) AS disk_allocated_bytes_max,
    max(disk_used_bytes) AS disk_used_bytes_max,
    min(network_egress_public_bytes) AS network_egress_public_bytes_min,
    max(network_egress_public_bytes) AS network_egress_public_bytes_max,
    min(network_egress_private_bytes) AS network_egress_private_bytes_min,
    max(network_egress_private_bytes) AS network_egress_private_bytes_max,
    min(network_ingress_public_bytes) AS network_ingress_public_bytes_min,
    max(network_ingress_public_bytes) AS network_ingress_public_bytes_max,
    min(network_ingress_private_bytes) AS network_ingress_private_bytes_min,
    max(network_ingress_private_bytes) AS network_ingress_private_bytes_max,
    toUInt64(count()) AS sample_count
FROM default.instance_checkpoints_v1
GROUP BY time, workspace_id, project_id, environment_id, resource_type, resource_id, container_uid, instance_id;

-- Per-container per-day usage aggregates (reads raw directly) ──────────
CREATE TABLE IF NOT EXISTS default.container_usage_per_day_v1
(
    `workspace_id` String,
    `project_id` LowCardinality(String),
    `environment_id` LowCardinality(String),
    `resource_type` LowCardinality(String),
    `resource_id` LowCardinality(String),
    `container_uid` String,
    `day` Date,
    `cpu_usage_usec_min` SimpleAggregateFunction(min, Int64),
    `cpu_usage_usec_max` SimpleAggregateFunction(max, Int64),
    `memory_bytes_sum` SimpleAggregateFunction(sum, Int64),
    `memory_bytes_max` SimpleAggregateFunction(max, Int64),
    `cpu_allocated_millicores_max` SimpleAggregateFunction(max, Int32),
    `memory_allocated_bytes_max` SimpleAggregateFunction(max, Int64),
    `disk_allocated_bytes_max` SimpleAggregateFunction(max, Int64),
    `disk_used_bytes_max` SimpleAggregateFunction(max, Int64),
    `network_egress_public_bytes_min` SimpleAggregateFunction(min, Int64),
    `network_egress_public_bytes_max` SimpleAggregateFunction(max, Int64),
    `network_egress_private_bytes_min` SimpleAggregateFunction(min, Int64),
    `network_egress_private_bytes_max` SimpleAggregateFunction(max, Int64),
    `network_ingress_public_bytes_min` SimpleAggregateFunction(min, Int64),
    `network_ingress_public_bytes_max` SimpleAggregateFunction(max, Int64),
    `network_ingress_private_bytes_min` SimpleAggregateFunction(min, Int64),
    `network_ingress_private_bytes_max` SimpleAggregateFunction(max, Int64),
    `ts_min` SimpleAggregateFunction(min, Int64),
    `ts_max` SimpleAggregateFunction(max, Int64),
    `sample_count` SimpleAggregateFunction(sum, UInt64)
)
ENGINE = AggregatingMergeTree()
ORDER BY (workspace_id, resource_id, container_uid, day)
TTL day + INTERVAL 400 DAY DELETE;

CREATE MATERIALIZED VIEW IF NOT EXISTS default.container_usage_per_day_mv_v1
TO default.container_usage_per_day_v1 AS
SELECT
    workspace_id, project_id, environment_id, resource_type, resource_id, container_uid,
    toDate(fromUnixTimestamp64Milli(ts)) AS day,
    min(cpu_usage_usec) AS cpu_usage_usec_min,
    max(cpu_usage_usec) AS cpu_usage_usec_max,
    sum(memory_bytes) AS memory_bytes_sum,
    max(memory_bytes) AS memory_bytes_max,
    max(cpu_allocated_millicores) AS cpu_allocated_millicores_max,
    max(memory_allocated_bytes) AS memory_allocated_bytes_max,
    max(disk_allocated_bytes) AS disk_allocated_bytes_max,
    max(disk_used_bytes) AS disk_used_bytes_max,
    min(network_egress_public_bytes) AS network_egress_public_bytes_min,
    max(network_egress_public_bytes) AS network_egress_public_bytes_max,
    min(network_egress_private_bytes) AS network_egress_private_bytes_min,
    max(network_egress_private_bytes) AS network_egress_private_bytes_max,
    min(network_ingress_public_bytes) AS network_ingress_public_bytes_min,
    max(network_ingress_public_bytes) AS network_ingress_public_bytes_max,
    min(network_ingress_private_bytes) AS network_ingress_private_bytes_min,
    max(network_ingress_private_bytes) AS network_ingress_private_bytes_max,
    min(ts) AS ts_min,
    max(ts) AS ts_max,
    toUInt64(count()) AS sample_count
FROM default.instance_checkpoints_v1
GROUP BY workspace_id, project_id, environment_id, resource_type, resource_id, container_uid, day;

-- Per-workspace per-month totals (reads container_usage_per_day_v1 — cascade) ─
-- Constant-time invoice queries: one row per (workspace, month).
CREATE TABLE IF NOT EXISTS default.workspace_usage_per_month_v1
(
    `workspace_id` String,
    `month` Date,
    `cpu_usec_total` SimpleAggregateFunction(sum, Int64),
    `memory_byte_ms_total` SimpleAggregateFunction(sum, Int64),
    `disk_allocated_byte_ms_total` SimpleAggregateFunction(sum, Int64),
    `network_egress_public_bytes_total` SimpleAggregateFunction(sum, Int64),
    `network_egress_private_bytes_total` SimpleAggregateFunction(sum, Int64),
    `network_ingress_public_bytes_total` SimpleAggregateFunction(sum, Int64),
    `network_ingress_private_bytes_total` SimpleAggregateFunction(sum, Int64),
    `peak_memory_bytes` SimpleAggregateFunction(max, Int64),
    `peak_disk_used_bytes` SimpleAggregateFunction(max, Int64)
    -- No sample_count: daily rows are already integrated, monthly is a
    -- plain sum with no divisor. Query container_usage_per_day_v1 directly
    -- for raw counts.
)
ENGINE = AggregatingMergeTree()
ORDER BY (workspace_id, month);

CREATE MATERIALIZED VIEW IF NOT EXISTS default.workspace_usage_per_month_mv_v1
TO default.workspace_usage_per_month_v1 AS
SELECT
    workspace_id,
    toStartOfMonth(day) AS month,
    sum(cpu_usage_usec_max - cpu_usage_usec_min) AS cpu_usec_total,
    sum((memory_bytes_sum / greatest(sample_count, 1)) * (ts_max - ts_min)) AS memory_byte_ms_total,
    sum(disk_allocated_bytes_max * (ts_max - ts_min)) AS disk_allocated_byte_ms_total,
    sum(network_egress_public_bytes_max - network_egress_public_bytes_min) AS network_egress_public_bytes_total,
    sum(network_egress_private_bytes_max - network_egress_private_bytes_min) AS network_egress_private_bytes_total,
    sum(network_ingress_public_bytes_max - network_ingress_public_bytes_min) AS network_ingress_public_bytes_total,
    sum(network_ingress_private_bytes_max - network_ingress_private_bytes_min) AS network_ingress_private_bytes_total,
    max(memory_bytes_max) AS peak_memory_bytes,
    max(disk_used_bytes_max) AS peak_disk_used_bytes
FROM default.container_usage_per_day_v1
GROUP BY workspace_id, month;

-- Query-friendly views ────────────────────────────────────────────────
-- Each view wraps the `_v1` table it shadows with FINAL + derived columns
-- so callers don't have to remember the aggregation math or the FINAL
-- requirement.

CREATE VIEW IF NOT EXISTS default.instance_checkpoints AS
SELECT * FROM default.instance_checkpoints_v1 FINAL;

CREATE VIEW IF NOT EXISTS default.container_usage_per_day AS
SELECT
    workspace_id,
    project_id,
    environment_id,
    resource_type,
    resource_id,
    container_uid,
    day,
    (cpu_usage_usec_max - cpu_usage_usec_min) / 1e6 AS cpu_seconds,
    (memory_bytes_sum / greatest(sample_count, 1)) * (ts_max - ts_min) AS memory_byte_ms,
    memory_bytes_max AS peak_memory_bytes,
    cpu_allocated_millicores_max AS peak_cpu_allocated_millicores,
    memory_allocated_bytes_max AS peak_memory_allocated_bytes,
    disk_allocated_bytes_max * (ts_max - ts_min) AS disk_allocated_byte_ms,
    disk_allocated_bytes_max AS peak_disk_allocated_bytes,
    disk_used_bytes_max AS peak_disk_used_bytes,
    network_egress_public_bytes_max - network_egress_public_bytes_min AS network_egress_public_bytes,
    network_egress_private_bytes_max - network_egress_private_bytes_min AS network_egress_private_bytes,
    network_ingress_public_bytes_max - network_ingress_public_bytes_min AS network_ingress_public_bytes,
    network_ingress_private_bytes_max - network_ingress_private_bytes_min AS network_ingress_private_bytes,
    sample_count
FROM default.container_usage_per_day_v1 FINAL;

CREATE VIEW IF NOT EXISTS default.workspace_usage_per_month AS
SELECT
    workspace_id,
    month,
    cpu_usec_total / 1e6 AS cpu_seconds,
    memory_byte_ms_total / 1000 AS memory_byte_seconds,
    disk_allocated_byte_ms_total / 1000 AS disk_allocated_byte_seconds,
    network_egress_public_bytes_total AS network_egress_public_bytes,
    network_egress_private_bytes_total AS network_egress_private_bytes,
    network_ingress_public_bytes_total AS network_ingress_public_bytes,
    network_ingress_private_bytes_total AS network_ingress_private_bytes,
    peak_memory_bytes,
    peak_disk_used_bytes
FROM default.workspace_usage_per_month_v1 FINAL;
