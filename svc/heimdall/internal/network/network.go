package network

import "k8s.io/apimachinery/pkg/types"

// Counters is the per-pod byte snapshot returned by Read. All four are
// monotonic accumulators (the tc program increments via
// __sync_fetch_and_add) so billing math is the same `max(counter) -
// min(counter)` shape as cpu_usage_usec.
type Counters struct {
	EgressPublicBytes   int64
	EgressPrivateBytes  int64
	IngressPublicBytes  int64
	IngressPrivateBytes int64
}

// zeroCounters is the all-zero Counters value returned by every fail-open
// path (read failed, attach failed, host-network pod, stub on macOS).
// Declared once here so callers don't need an exhaustruct-shaped literal
// at every fail site.
var zeroCounters = Counters{
	EgressPublicBytes:   0,
	EgressPrivateBytes:  0,
	IngressPublicBytes:  0,
	IngressPrivateBytes: 0,
}

// Reader is the per-process handle the collector talks to. The Linux
// implementation owns the loaded eBPF program plus the per-pod attach
// links; the non-Linux implementation is a no-op so the same code path
// compiles on macOS and in tests.
//
// All methods are safe for concurrent use.
type Reader interface {
	// Attach resolves the pod's CNI netns through containerd (sandbox
	// container's OCI spec carries the netns path, which works uniformly
	// for runc and gVisor pods - gVisor's CNI netns has no pod IP on eth0
	// so we can't match by IP) and hooks the two tc programs onto the
	// host-side veth peer of eth0 inside that netns.
	// Idempotent - re-attaching a pod that's already attached is a no-op.
	Attach(uid types.UID) error

	// Detach removes the tc attachments and drops the pod's BPF map
	// entry. Safe to call on a pod that was never attached.
	Detach(uid types.UID)

	// Read returns the current cumulative byte counters for the pod.
	// Returns zeros (no error) when the pod isn't attached or has not
	// yet seen any packets.
	Read(uid types.UID) (Counters, error)

	// Close releases all attach links and frees the BPF map. Idempotent.
	Close() error
}

// NewReader constructs the platform-appropriate Reader. On Linux it loads
// the eBPF program and dials containerd at criSocket for sandbox lookups.
// On other platforms it returns a no-op stub. Failure to load the program
// (missing capabilities, no BTF, etc.) is fatal: a silently-broken
// collector would undercharge every pod for network.
func NewReader(criSocket string) (Reader, error) {
	return newReader(criSocket)
}
