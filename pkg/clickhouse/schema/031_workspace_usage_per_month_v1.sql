-- Per-workspace per-month usage totals. Populated by a MATERIALIZED VIEW
-- that reads from container_usage_per_day_v1 (one level up the cascade).
--
-- Why: invoice queries run once per workspace per billing period. At scale
-- that query can't afford to scan every container's daily rows — a busy
-- workspace has thousands of container_uids × 30 days. This table pre-sums
-- to one row per (workspace, month) so the invoice query is effectively a
-- primary-key lookup.
--
-- Columns here are pre-differenced SCALARS (byte-ms, cpu-μs totals) because
-- counters can't be aggregated across container_uids. The daily MV did the
-- per-container max-min dance; this layer just sums.
CREATE TABLE workspace_usage_per_month_v1 (
  workspace_id String,
  month Date,
  -- Totals summed across every container's per-day row in the month.
  cpu_usec_total SimpleAggregateFunction(sum, Int64),
  memory_byte_ms_total SimpleAggregateFunction(sum, Int64),
  disk_allocated_byte_ms_total SimpleAggregateFunction(sum, Int64),
  network_egress_public_bytes_total SimpleAggregateFunction(sum, Int64),
  network_egress_private_bytes_total SimpleAggregateFunction(sum, Int64),
  network_ingress_public_bytes_total SimpleAggregateFunction(sum, Int64),
  network_ingress_private_bytes_total SimpleAggregateFunction(sum, Int64),
  -- Peaks retained for capacity/alerting dashboards.
  peak_memory_bytes SimpleAggregateFunction(max, Int64),
  peak_disk_used_bytes SimpleAggregateFunction(max, Int64)
  -- Note: no sample_count here. Daily rows are already integrated
  -- (memory_byte_ms etc.) so the monthly aggregate is a plain sum — no
  -- divisor needed. If you need the raw sample count for diagnostics,
  -- query container_usage_per_day_v1 directly.
)
ENGINE = AggregatingMergeTree()
ORDER BY (workspace_id, month);
