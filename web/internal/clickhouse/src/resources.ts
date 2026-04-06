import { z } from "zod";
import type { Querier } from "./client";

// ─── Time window config ────────────────────────────────────────────────
//
// Each window picks the cheapest MV that can serve the requested range
// without producing an unreadable pixel-soup chart (target ~60-1500
// buckets per chart). Shorter windows read the per-15s MV for granularity;
// multi-day windows read per-minute; weekly reads per-hour.

export const TIME_WINDOWS = ["15m", "1h", "3h", "6h", "12h", "1d", "1w"] as const;
export type TimeWindow = (typeof TIME_WINDOWS)[number];

type WindowSpec = {
  windowSeconds: number;
  bucketSeconds: number;
  mvTable: string;
};

const WINDOW_CONFIG: Record<TimeWindow, WindowSpec> = {
  "15m": {
    windowSeconds: 15 * 60,
    bucketSeconds: 15,
    mvTable: "default.instance_resources_per_15s_v1",
  },
  "1h": {
    windowSeconds: 60 * 60,
    bucketSeconds: 15,
    mvTable: "default.instance_resources_per_15s_v1",
  },
  "3h": {
    windowSeconds: 3 * 60 * 60,
    bucketSeconds: 60,
    mvTable: "default.instance_resources_per_minute_v1",
  },
  "6h": {
    windowSeconds: 6 * 60 * 60,
    bucketSeconds: 60,
    mvTable: "default.instance_resources_per_minute_v1",
  },
  "12h": {
    windowSeconds: 12 * 60 * 60,
    bucketSeconds: 60,
    mvTable: "default.instance_resources_per_minute_v1",
  },
  "1d": {
    windowSeconds: 24 * 60 * 60,
    bucketSeconds: 60,
    mvTable: "default.instance_resources_per_minute_v1",
  },
  "1w": {
    windowSeconds: 7 * 24 * 60 * 60,
    bucketSeconds: 60 * 60,
    mvTable: "default.instance_resources_per_hour_v1",
  },
};

// Raw FINAL view for the "live tip" of charts (current, still-filling bucket).
const CHECKPOINTS_VIEW = "default.instance_checkpoints";

// `instance_id` in the checkpoints schema is the k8s pod name. Filtering by
// it scopes results to a single replica (e.g., the instance the user clicked
// in the network panel) instead of summing across all of the deployment's
// pods. Empty string is the "no filter" sentinel because all real pod names
// have at least one character — keeps the WHERE clause stable so the query
// plan caches.
const RESOURCE_FILTER = `
  workspace_id = {workspaceId: String}
  AND resource_type = {resourceType: String}
  AND resource_id = {resourceId: String}
  AND ({instanceName: String} = '' OR instance_id = {instanceName: String})`;

const baseParams = z.object({
  workspaceId: z.string(),
  resourceType: z.enum(["deployment", "sentinel"]),
  resourceId: z.string(),
  instanceName: z.string().default(""),
  window: z.enum(TIME_WINDOWS).default("1h"),
});

const timeseriesPointSchema = z.object({
  x: z.number().int(),
  y: z.number(),
});

// ─── Hybrid timeseries (MV for cold tail + raw for live bucket) ───────
//
// Chart data comes from two places stitched with UNION ALL:
//   - Cold tail: the pre-computed MV for all closed buckets in the window.
//   - Live tip: raw instance_checkpoints for the current (still-filling)
//     bucket. Same per-bucket math as the MV, but computed on demand so
//     new checkpoints show up within one refresh interval.
//
// The current bucket is excluded from the MV branch with
//   time < toStartOfInterval(now(), INTERVAL {bucketSeconds} SECOND)
// and emitted from the raw branch as a single row keyed at that boundary.
// The outer WHERE y > 0 drops the live-tip row when no checkpoints have
// landed in the current bucket yet (otherwise the chart shows a phantom
// "0" bar at the right edge until the first sample arrives).

function buildParams() {
  return baseParams.extend({
    windowSeconds: z.number(),
    bucketSeconds: z.number(),
  });
}

function specFor(args: z.infer<typeof baseParams>) {
  const spec = WINDOW_CONFIG[args.window];
  return {
    ...args,
    windowSeconds: spec.windowSeconds,
    bucketSeconds: spec.bucketSeconds,
    mvTable: spec.mvTable,
  };
}

// ─── Metric registry ──────────────────────────────────────────────────
//
// Each metric describes how to compute a per-bucket y-value from the MV
// (cold tail) and from raw checkpoints (live tip). Two shapes:
//
//   - Container-reduce-then-aggregate (cpu, memory, disk): the live-tip
//     query first reduces raw rows per container_uid via `perContainer`,
//     then sums across containers via `rawAgg` (which references the
//     `container_value` alias produced by the inner subquery).
//
//   - Direct aggregation (instances): the live-tip query aggregates
//     directly over raw rows. `perContainer` is omitted.
//
// Adding a metric = one entry below. The hybrid query structure, MV-vs-
// raw stitching, parameter binding, and zero-bar suppression are shared.

type Metric = {
  // Per-bucket aggregate over the MV's pre-aggregated columns.
  // Resulting column is aliased AS y by the builder.
  mvAgg: string;
  // Per-bucket aggregate over either raw checkpoints or the per-container
  // subquery output. Aliased AS y by the builder.
  rawAgg: string;
  // Optional: per-container reduction inside the live-tip subquery.
  // Result is aliased AS container_value, available to rawAgg.
  perContainer?: string;
};

const METRICS: Record<string, Metric> = {
  // CPU: cumulative usec counter delta per container (= microseconds the
  // container actually spent on-CPU during the bucket), summed across
  // containers, divided by bucket length and 1000 → millicores.
  cpu: {
    mvAgg: "sum(cpu_usage_usec_max - cpu_usage_usec_min) / {bucketSeconds: UInt32} / 1000",
    perContainer: "max(cpu_usage_usec) - min(cpu_usage_usec)",
    rawAgg: "sum(container_value) / {bucketSeconds: UInt32} / 1000",
  },
  // Memory: peak (working-set) bytes per container during the bucket,
  // summed across containers. max() is monotone so retries don't double-count.
  memory: {
    mvAgg: "sum(memory_bytes_max)",
    perContainer: "max(memory_bytes)",
    rawAgg: "sum(container_value)",
  },
  // Disk: latest used bytes per container during the bucket, summed
  // across containers. Same shape as memory.
  disk: {
    mvAgg: "sum(disk_used_bytes_max)",
    perContainer: "max(disk_used_bytes)",
    rawAgg: "sum(container_value)",
  },
  // Active instances: distinct container_uid count per bucket. No
  // per-container reduce — uniq is already a direct aggregation.
  instances: {
    mvAgg: "uniq(container_uid)",
    rawAgg: "uniq(container_uid)",
  },
  // Network egress: sum of public + private egress bytes counter delta per
  // bucket (max - min, monotone like cpu_usage_usec), divided by bucket
  // length to give bytes/sec. Counters come from the cgroup_skb eBPF
  // collector. v1 sums public + private into one rate; we can split into
  // two stacked colours later if customers need to see the breakdown.
  network_egress: {
    mvAgg:
      "sum((network_egress_public_bytes_max - network_egress_public_bytes_min) + (network_egress_private_bytes_max - network_egress_private_bytes_min)) / {bucketSeconds: UInt32}",
    perContainer:
      "(max(network_egress_public_bytes) - min(network_egress_public_bytes)) + (max(network_egress_private_bytes) - min(network_egress_private_bytes))",
    rawAgg: "sum(container_value) / {bucketSeconds: UInt32}",
  },
  network_ingress: {
    mvAgg:
      "sum((network_ingress_public_bytes_max - network_ingress_public_bytes_min) + (network_ingress_private_bytes_max - network_ingress_private_bytes_min)) / {bucketSeconds: UInt32}",
    perContainer:
      "(max(network_ingress_public_bytes) - min(network_ingress_public_bytes)) + (max(network_ingress_private_bytes) - min(network_ingress_private_bytes))",
    rawAgg: "sum(container_value) / {bucketSeconds: UInt32}",
  },
};

function buildHybridQuery(metric: Metric, mvTable: string): string {
  const liveTipFrom = metric.perContainer
    ? `FROM (
        SELECT
          container_uid,
          ${metric.perContainer} AS container_value
        FROM ${CHECKPOINTS_VIEW}
        WHERE ${RESOURCE_FILTER}
          AND ts >= toUnixTimestamp(
            toStartOfInterval(now(), INTERVAL {bucketSeconds: UInt32} SECOND)
          ) * 1000
        GROUP BY container_uid
      )`
    : `FROM ${CHECKPOINTS_VIEW}
       WHERE ${RESOURCE_FILTER}
         AND ts >= toUnixTimestamp(
           toStartOfInterval(now(), INTERVAL {bucketSeconds: UInt32} SECOND)
         ) * 1000`;

  return `
    SELECT toInt64(toUnixTimestamp(time) * 1000) AS x, toFloat64(y) AS y
    FROM (
      SELECT
        time,
        ${metric.mvAgg} AS y
      FROM ${mvTable}
      WHERE ${RESOURCE_FILTER}
        AND time >= now() - INTERVAL {windowSeconds: UInt32} SECOND
        AND time < toStartOfInterval(now(), INTERVAL {bucketSeconds: UInt32} SECOND)
      GROUP BY time

      UNION ALL

      SELECT time, y FROM (
        SELECT
          toStartOfInterval(now(), INTERVAL {bucketSeconds: UInt32} SECOND) AS time,
          ${metric.rawAgg} AS y
        ${liveTipFrom}
      ) WHERE y > 0
    )
    ORDER BY x ASC`;
}

function makeTimeseriesQuery(metric: Metric) {
  return (ch: Querier) => async (args: z.infer<typeof baseParams>) => {
    const { mvTable, ...params } = specFor(args);
    const query = ch.query({
      query: buildHybridQuery(metric, mvTable),
      params: buildParams(),
      schema: timeseriesPointSchema,
    });
    return query(params);
  };
}

export const getResourceCpuTimeseries = makeTimeseriesQuery(METRICS.cpu);
export const getResourceMemoryTimeseries = makeTimeseriesQuery(METRICS.memory);
export const getResourceDiskTimeseries = makeTimeseriesQuery(METRICS.disk);
export const getResourceInstanceCountTimeseries = makeTimeseriesQuery(METRICS.instances);
export const getResourceNetworkEgressTimeseries = makeTimeseriesQuery(METRICS.network_egress);
export const getResourceNetworkIngressTimeseries = makeTimeseriesQuery(METRICS.network_ingress);

// ─── Current Resource Summary (latest window from raw FINAL view) ─────
//
// Independent of the chart's time window — always reports "right now" from
// raw data. The panel header should feel live (~seconds) regardless of
// whether the chart is showing the past 15 minutes or the past week.
const RATE_WINDOW_MINUTES = 2;
const LIVE_WINDOW_SECONDS = 30;

const resourceSummarySchema = z.object({
  active_instances: z.number().int(),
  current_cpu_millicores: z.number(),
  current_memory_bytes: z.number().int(),
  current_disk_used_bytes: z.number().int(),
  current_egress_bytes_per_sec: z.number(),
  current_ingress_bytes_per_sec: z.number(),
  cpu_allocated_millicores: z.number(),
  memory_allocated_bytes: z.number(),
});

const summaryParams = z.object({
  workspaceId: z.string(),
  resourceType: z.enum(["deployment", "sentinel"]),
  resourceId: z.string(),
  instanceName: z.string().default(""),
});

export function getResourceSummary(ch: Querier) {
  return async (args: z.infer<typeof summaryParams>) => {
    const query = ch.query({
      query: `
        WITH per_container AS (
          SELECT
            container_uid,
            max(ts) AS last_seen_ts,
            max(cpu_usage_usec) - min(cpu_usage_usec) AS cpu_delta_usec,
            greatest(max(ts) - min(ts), 1) AS span_ms,
            argMax(memory_bytes, ts) AS memory_bytes_latest,
            argMax(disk_used_bytes, ts) AS disk_used_bytes_latest,
            argMax(cpu_allocated_millicores, ts) AS cpu_alloc_latest,
            argMax(memory_allocated_bytes, ts) AS memory_alloc_latest,
            (max(network_egress_public_bytes) - min(network_egress_public_bytes))
              + (max(network_egress_private_bytes) - min(network_egress_private_bytes)) AS egress_delta_bytes,
            (max(network_ingress_public_bytes) - min(network_ingress_public_bytes))
              + (max(network_ingress_private_bytes) - min(network_ingress_private_bytes)) AS ingress_delta_bytes
          FROM ${CHECKPOINTS_VIEW}
          WHERE ${RESOURCE_FILTER}
            AND ts >= toUnixTimestamp(now() - INTERVAL {rateWindowMinutes: UInt8} MINUTE) * 1000
          GROUP BY container_uid
        ),
        live_cutoff AS (
          SELECT toUnixTimestamp(now() - INTERVAL {liveWindowSeconds: UInt16} SECOND) * 1000 AS cutoff_ms
        )
        SELECT
          toUInt32(countIf(last_seen_ts >= (SELECT cutoff_ms FROM live_cutoff))) AS active_instances,
          sumIf(cpu_delta_usec / span_ms, last_seen_ts >= (SELECT cutoff_ms FROM live_cutoff)) AS current_cpu_millicores,
          toUInt64(sumIf(memory_bytes_latest, last_seen_ts >= (SELECT cutoff_ms FROM live_cutoff))) AS current_memory_bytes,
          toUInt64(sumIf(disk_used_bytes_latest, last_seen_ts >= (SELECT cutoff_ms FROM live_cutoff))) AS current_disk_used_bytes,
          sumIf(egress_delta_bytes / (span_ms / 1000), last_seen_ts >= (SELECT cutoff_ms FROM live_cutoff)) AS current_egress_bytes_per_sec,
          sumIf(ingress_delta_bytes / (span_ms / 1000), last_seen_ts >= (SELECT cutoff_ms FROM live_cutoff)) AS current_ingress_bytes_per_sec,
          sumIf(cpu_alloc_latest, last_seen_ts >= (SELECT cutoff_ms FROM live_cutoff)) AS cpu_allocated_millicores,
          sumIf(memory_alloc_latest, last_seen_ts >= (SELECT cutoff_ms FROM live_cutoff)) AS memory_allocated_bytes
        FROM per_container`,
      params: summaryParams.extend({
        rateWindowMinutes: z.number(),
        liveWindowSeconds: z.number(),
      }),
      schema: resourceSummarySchema,
    });

    return query({
      ...args,
      rateWindowMinutes: RATE_WINDOW_MINUTES,
      liveWindowSeconds: LIVE_WINDOW_SECONDS,
    });
  };
}
