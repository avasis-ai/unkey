-- Per-container per-day usage aggregates. The lowest-grain billing source.
-- Populated by a MATERIALIZED VIEW reading directly from instance_checkpoints_v1.
--
-- Why per-container (not per-deployment): billing math is
--   max(cpu_usage_usec) - min(cpu_usage_usec)
-- which only works within a single container lifetime. container_uid =
-- pod_uid + restart_count; the kernel counter resets at each restart.
-- Grouping at the deployment level here would mix counters across resets
-- and silently overcharge. Roll-ups (deployment, project, workspace) are
-- SUMs on top of this table — that's what workspace_usage_per_month_v1
-- does one level up for invoices.
--
-- Memory approximation: sum/count × window. At the default 10s poll cadence
-- this is within a few % of the exact integral; matches GKE's byte-seconds
-- convention and tends to undercharge on transient spikes.
CREATE TABLE container_usage_per_day_v1 (
  workspace_id String,
  project_id LowCardinality(String),
  environment_id LowCardinality(String),
  resource_type LowCardinality(String),
  resource_id LowCardinality(String),
  container_uid String,
  day Date,
  cpu_usage_usec_min SimpleAggregateFunction(min, Int64),
  cpu_usage_usec_max SimpleAggregateFunction(max, Int64),
  memory_bytes_sum SimpleAggregateFunction(sum, Int64),
  memory_bytes_max SimpleAggregateFunction(max, Int64),
  cpu_allocated_millicores_max SimpleAggregateFunction(max, Int32),
  memory_allocated_bytes_max SimpleAggregateFunction(max, Int64),
  disk_allocated_bytes_max SimpleAggregateFunction(max, Int64),
  disk_used_bytes_max SimpleAggregateFunction(max, Int64),
  network_egress_public_bytes_min SimpleAggregateFunction(min, Int64),
  network_egress_public_bytes_max SimpleAggregateFunction(max, Int64),
  network_egress_private_bytes_min SimpleAggregateFunction(min, Int64),
  network_egress_private_bytes_max SimpleAggregateFunction(max, Int64),
  network_ingress_public_bytes_min SimpleAggregateFunction(min, Int64),
  network_ingress_public_bytes_max SimpleAggregateFunction(max, Int64),
  network_ingress_private_bytes_min SimpleAggregateFunction(min, Int64),
  network_ingress_private_bytes_max SimpleAggregateFunction(max, Int64),
  ts_min SimpleAggregateFunction(min, Int64),
  ts_max SimpleAggregateFunction(max, Int64),
  sample_count SimpleAggregateFunction(sum, UInt64)
)
ENGINE = AggregatingMergeTree()
ORDER BY (workspace_id, resource_id, container_uid, day)
TTL day + INTERVAL 400 DAY DELETE;
