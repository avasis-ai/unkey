package collector

import (
	"context"
	"errors"
	"os"

	"github.com/unkeyed/unkey/pkg/clickhouse/schema"
	"github.com/unkeyed/unkey/pkg/logger"
	"github.com/unkeyed/unkey/svc/heimdall/internal/checkpoint"
	"github.com/unkeyed/unkey/svc/heimdall/internal/network"
	"github.com/unkeyed/unkey/svc/heimdall/pkg/metrics"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
)

// podInfo carries everything the collector needs to build a checkpoint for
// one container. It is populated from the informer cache at the start of each
// tick (we never block a cgroup read on an API call).
type podInfo struct {
	name                   string
	uid                    types.UID
	qosClass               corev1.PodQOSClass
	workspaceID            string
	projectID              string
	environmentID          string
	resourceType           string
	resourceID             string
	restartCount           int32
	hostNetwork            bool
	podIP                  string          // primary IPv4 assigned by the CNI; used to find the pod's CNI netns at tc attach time
	phase                  corev1.PodPhase // skip network attach for non-Running pods (sandbox container is gone after Completion)
	cpuAllocatedMillicores int32
	memoryAllocatedBytes   int64
	diskAllocatedBytes     int64
}

// collect runs one tick: list krane pods on this node, read their cgroup
// counters, and buffer one checkpoint per container. All billing math is
// deferred to query time.
func (c *Collector) collect(_ context.Context) error {
	now := c.clk.Now().UnixMilli()
	pods := c.buildKranePodLookup()

	var written int
	for _, info := range pods {
		reading, err := c.cgroup.read(info.uid, info.qosClass)
		if err != nil {
			if !errors.Is(err, os.ErrNotExist) {
				metrics.CgroupReadErrors.Inc()
				logger.Error("cgroup read failed", "pod", info.name, "error", err.Error())
			}
			continue
		}

		containerUID := checkpoint.ContainerUID(string(info.uid), info.restartCount)
		var diskUsed int64
		if c.kubeletRoot != "" && info.diskAllocatedBytes > 0 {
			diskUsed = readEphemeralUsedBytes(c.kubeletRoot, info.uid)
		}
		net := c.attachAndReadNetwork(info)
		c.ch.Buffer(schema.InstanceCheckpoint{
			NodeID:                     c.nodeName,
			WorkspaceID:                info.workspaceID,
			ProjectID:                  info.projectID,
			EnvironmentID:              info.environmentID,
			ResourceType:               info.resourceType,
			ResourceID:                 info.resourceID,
			PodUID:                     string(info.uid),
			InstanceID:                 info.name,
			ContainerUID:               containerUID,
			RestartCount:               uint32(info.restartCount),
			Ts:                         now,
			EventKind:                  checkpoint.EventPeriodic,
			CPUUsageUsec:               reading.cpuUsageUsec,
			MemoryBytes:                reading.memoryBytes,
			CPUAllocatedMillicores:     info.cpuAllocatedMillicores,
			MemoryAllocatedBytes:       info.memoryAllocatedBytes,
			DiskAllocatedBytes:         info.diskAllocatedBytes,
			DiskUsedBytes:              diskUsed,
			NetworkEgressPublicBytes:   net.EgressPublicBytes,
			NetworkEgressPrivateBytes:  net.EgressPrivateBytes,
			NetworkIngressPublicBytes:  net.IngressPublicBytes,
			NetworkIngressPrivateBytes: net.IngressPrivateBytes,
			Region:                     c.region,
			Platform:                   c.platform,
		})
		written++
	}

	metrics.KranePods.Set(float64(len(pods)))
	logger.Info("collection tick",
		"node", c.nodeName,
		"krane_pods", len(pods),
		"checkpoints_written", written,
	)
	return nil
}

func (c *Collector) buildKranePodLookup() map[string]podInfo {
	pods := make(map[string]podInfo)
	allPods, err := c.podLister.List(labels.Everything())
	if err != nil {
		logger.Error("failed to list pods from cache", "error", err.Error())
		return pods
	}

	for _, pod := range allPods {
		if pod.Spec.NodeName != c.nodeName {
			continue
		}
		if !isBillablePod(pod) {
			continue
		}
		pods[string(pod.UID)] = buildPodInfo(pod)
	}

	return pods
}

// isBillablePod returns true if pod is a krane-managed deployment or a sentinel.
func isBillablePod(pod *corev1.Pod) bool {
	component := pod.Labels[LabelComponent]
	if component == "deployment" && pod.Labels[LabelManagedBy] != "krane" {
		return false
	}
	return component == "deployment" || component == "sentinel"
}

// buildPodInfo extracts the billing-relevant fields from a pod.
func buildPodInfo(pod *corev1.Pod) podInfo {
	component := pod.Labels[LabelComponent]
	resourceID := pod.Labels[LabelDeployment]
	if component == "sentinel" {
		resourceID = pod.Labels[LabelSentinel]
	}
	cpuMilli, memBytes := primaryContainerAllocation(pod)
	return podInfo{
		name:                   pod.Name,
		uid:                    pod.UID,
		qosClass:               pod.Status.QOSClass,
		workspaceID:            pod.Labels[LabelWorkspace],
		projectID:              pod.Labels[LabelProject],
		environmentID:          pod.Labels[LabelEnv],
		resourceType:           component,
		resourceID:             resourceID,
		restartCount:           primaryContainerRestartCount(pod),
		hostNetwork:            pod.Spec.HostNetwork,
		podIP:                  pod.Status.PodIP,
		phase:                  pod.Status.Phase,
		cpuAllocatedMillicores: cpuMilli,
		memoryAllocatedBytes:   memBytes,
		diskAllocatedBytes:     ephemeralStorageBytes(pod),
	}
}

// primaryContainerAllocation returns the CPU (millicores) and memory (bytes)
// limits declared on the pod's primary container. We prefer Limits over
// Requests because Limits is the true ceiling, which is what the customer is paying
// for and can actually use. Requests is a scheduling hint (often set lower
// for bin-packing, e.g., krane sets Requests = Limits / 4). For utilization
// dashboards ("how much of my capacity am I using"), the denominator that
// matters is the Limit, not the Request.
// If Limits is absent, fall back to Requests.
func primaryContainerAllocation(pod *corev1.Pod) (int32, int64) {
	if len(pod.Spec.Containers) == 0 {
		return 0, 0
	}
	c := pod.Spec.Containers[0]

	cpu := c.Resources.Limits.Cpu()
	if cpu == nil || cpu.IsZero() {
		cpu = c.Resources.Requests.Cpu()
	}
	mem := c.Resources.Limits.Memory()
	if mem == nil || mem.IsZero() {
		mem = c.Resources.Requests.Memory()
	}

	var cpuMilli int32
	if cpu != nil {
		cpuMilli = int32(cpu.MilliValue())
	}
	var memBytes int64
	if mem != nil {
		memBytes = mem.Value()
	}
	return cpuMilli, memBytes
}

// primaryContainerRestartCount returns the restart count of the pod's primary
// container, the one whose name appears first in Spec.Containers. We look it
// up by name (not status index) because Status.ContainerStatuses ordering is
// not part of the K8s API contract: sidecar restarts can reshuffle indices
// and silently change the container_uid we bill against, causing overcharge.
func primaryContainerRestartCount(pod *corev1.Pod) int32 {
	name := primaryContainerName(pod)
	if name == "" {
		return 0
	}
	for i := range pod.Status.ContainerStatuses {
		if pod.Status.ContainerStatuses[i].Name == name {
			return pod.Status.ContainerStatuses[i].RestartCount
		}
	}
	return 0
}

// primaryContainerName returns the name of the primary container. First in
// Spec.Containers (spec order is stable; only the declared order changes with
// new deployments, never transiently).
func primaryContainerName(pod *corev1.Pod) string {
	if len(pod.Spec.Containers) == 0 {
		return ""
	}
	return pod.Spec.Containers[0].Name
}

// zeroCounters is the all-zero value the fail-open paths below return.
// Declared once so we don't repeat the exhaustruct-shaped literal four
// times. Mirrors network.zeroCounters which serves the same role inside
// the network package.
var zeroCounters = network.Counters{
	EgressPublicBytes:   0,
	EgressPrivateBytes:  0,
	IngressPublicBytes:  0,
	IngressPrivateBytes: 0,
}

// attachAndReadNetwork lazily attaches the eBPF cgroup_skb counters on first
// observation of a pod, then reads the current snapshot. Host-network pods
// (heimdall itself, kube-proxy, sentinels) share the host net namespace and
// have no cgroup-scoped traffic worth attributing, so they're skipped. Read
// failures fail open (return zero counters) so a flaky eBPF map can never
// take down the rest of the checkpoint.
func (c *Collector) attachAndReadNetwork(info podInfo) network.Counters {
	if c.network == nil || info.hostNetwork || info.podIP == "" {
		return zeroCounters
	}
	// Completed/Failed pods keep showing up in the informer cache until
	// kubelet GC runs, but their sandbox containers are already gone, so any
	// Attach call would log a warn and churn retries. Skip them: if they
	// were previously attached, the final pre-exit map read already happened.
	if info.phase != corev1.PodRunning {
		return zeroCounters
	}
	if err := c.network.Attach(info.uid); err != nil {
		// Attach is idempotent so this is genuinely a new failure (e.g.,
		// CNI netns raced away, veth not ready yet). Log at warn so
		// transient startup races are visible but don't spam steady-state.
		logger.Warn("network attach failed", "pod", info.name, "error", err.Error())
		return zeroCounters
	}
	counters, err := c.network.Read(info.uid)
	if err != nil {
		return zeroCounters
	}
	return counters
}

// ephemeralStorageBytes returns the allocated storage size (bytes) requested
// by the first ephemeral volume in the pod spec. Non-ephemeral pods return 0.
func ephemeralStorageBytes(pod *corev1.Pod) int64 {
	for _, vol := range pod.Spec.Volumes {
		if vol.Ephemeral == nil || vol.Ephemeral.VolumeClaimTemplate == nil {
			continue
		}
		req := vol.Ephemeral.VolumeClaimTemplate.Spec.Resources.Requests
		if q, ok := req[corev1.ResourceStorage]; ok {
			if v, ok := q.AsInt64(); ok && v > 0 {
				return v
			}
		}
	}
	return 0
}
