-- Query-friendly view over container_usage_per_day_v1. Applies FINAL and
-- surfaces derived columns (cpu_seconds, memory_byte_ms, disk_allocated_byte_ms,
-- network deltas) so callers don't have to remember the aggregation math.
--
-- Query pattern — billing/invoicing drill-downs:
--
--   SELECT workspace_id,
--          sum(cpu_seconds),
--          sum(memory_byte_ms) / 1000 AS mem_byte_seconds
--   FROM container_usage_per_day
--   WHERE day BETWEEN '2026-04-01' AND '2026-04-30'
--   GROUP BY workspace_id
--
-- For the single-shot monthly invoice case use workspace_usage_per_month
-- instead — it's constant-time per workspace.
CREATE VIEW container_usage_per_day AS
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
FROM container_usage_per_day_v1 FINAL;
