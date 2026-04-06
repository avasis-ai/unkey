package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// CollectionTotal counts collection ticks by result.
	CollectionTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "unkey",
			Subsystem: "heimdall",
			Name:      "collection_total",
			Help:      "Total number of collection ticks.",
		},
		[]string{"result"}, // "success", "error"
	)

	// CollectionDuration tracks how long each collection tick takes.
	CollectionDuration = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "unkey",
			Subsystem: "heimdall",
			Name:      "collection_duration_seconds",
			Help:      "Duration of collection ticks in seconds.",
			Buckets:   []float64{0.01, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10},
		},
	)

	// KranePods tracks the current number of krane-managed pods seen on this node.
	KranePods = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "unkey",
			Subsystem: "heimdall",
			Name:      "krane_pods",
			Help:      "Current number of krane-managed pods on this node.",
		},
	)

	// CgroupReadErrors counts cgroup file read failures.
	CgroupReadErrors = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "unkey",
			Subsystem: "heimdall",
			Name:      "cgroup_read_errors_total",
			Help:      "Total number of cgroup file read failures.",
		},
	)

	// LifecycleEmitted counts CRI lifecycle checkpoints successfully buffered.
	// kind is "start" or "stop".
	LifecycleEmitted = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "unkey",
			Subsystem: "heimdall",
			Name:      "lifecycle_checkpoints_emitted_total",
			Help:      "Total number of CRI lifecycle checkpoints written to the buffer.",
		},
		[]string{"kind"}, // "start", "stop"
	)

	// LifecycleDrops counts CRI lifecycle events we could not emit a
	// checkpoint for. kind is the event type; reason explains the drop so we
	// can distinguish informer races from cgroup-teardown races.
	LifecycleDrops = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "unkey",
			Subsystem: "heimdall",
			Name:      "lifecycle_checkpoints_dropped_total",
			Help:      "Total CRI lifecycle events that did not produce a checkpoint. Each drop is a bounded undercharge.",
		},
		[]string{"kind", "reason"}, // kind: "start"|"stop"; reason: "pod_not_found"|"cgroup_read_failed"
	)
)
