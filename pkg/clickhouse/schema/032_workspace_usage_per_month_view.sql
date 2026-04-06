-- Query-friendly view over workspace_usage_per_month_v1. Applies FINAL and
-- converts byte-ms → byte-seconds so invoice math is a direct multiply.
--
--   SELECT workspace_id,
--          cpu_seconds * 0.00000772 AS cpu_dollars,
--          memory_byte_seconds * 0.00000386 / 1e9 AS memory_dollars,
--          disk_allocated_byte_seconds * 0.00000006 / 1e9 AS disk_dollars
--   FROM workspace_usage_per_month
--   WHERE workspace_id = ? AND month = '2026-04-01'
CREATE VIEW workspace_usage_per_month AS
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
FROM workspace_usage_per_month_v1 FINAL;
