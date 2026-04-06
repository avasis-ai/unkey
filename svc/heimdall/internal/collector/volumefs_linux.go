//go:build linux

package collector

import (
	"path/filepath"
	"syscall"

	"k8s.io/apimachinery/pkg/types"
)

// readEphemeralUsedBytes returns the sum of used bytes across every per-pod
// volume directory kubelet mounts under <kubeletRoot>/pods/<uid>. Returns 0
// when the pod has no volumes, the kubelet hostPath isn't mounted into the
// heimdall container, or the pod has just terminated.
//
// The expected layout (production: ebs-csi-gp3; dev: TopoLVM) is a real CSI
// volume that exposes its own filesystem at <entry>/mount on its own block
// device. statfs there returns this volume's bytes only — exact, cheap,
// correct for billing.
//
// Bind-mount-style "fake" CSI drivers (csi-hostpath-driver, in-tree hostPath)
// are explicitly NOT supported: their `mount` path shares the host fs Dev
// with the kubelet root, so statfs would report node-wide stats spanning
// every pod. Skipping them undercharges (safe direction) and avoids
// accidentally billing the customer for the host's used bytes.
//
// Path shape (set by kubelet; stable across K8s versions):
//
//	<kubeletRoot>/pods/<pod_uid>/volumes/<provider>/<volume_name>/mount
func readEphemeralUsedBytes(kubeletRoot string, uid types.UID) int64 {
	pattern := filepath.Join(kubeletRoot, "pods", string(uid), "volumes", "*", "*", "mount")
	mounts, err := filepath.Glob(pattern)
	if err != nil || len(mounts) == 0 {
		return 0
	}

	rootDev, ok := statDev(kubeletRoot)
	if !ok {
		// We can't tell real volumes from bind mounts without the root
		// Dev — skip rather than risk over-reporting.
		return 0
	}

	var total int64
	for _, mount := range mounts {
		dev, ok := statDev(mount)
		if !ok || dev == rootDev {
			// Bind mount on the host fs — reading statfs would lie.
			continue
		}
		total += statfsUsedBytes(mount)
	}
	return total
}

// statDev returns the device id for path, or (0, false) if stat fails.
// Bind mounts preserve the source fs's Dev, so a Dev that differs from the
// kubelet root identifies a real per-volume filesystem.
func statDev(path string) (uint64, bool) {
	var st syscall.Stat_t
	if err := syscall.Stat(path, &st); err != nil {
		return 0, false
	}
	return uint64(st.Dev), true
}

// statfsUsedBytes returns (Blocks - Bavail) * Bsize, matching what `df`
// reports — includes filesystem overhead (journal, reserved blocks).
// Customer allocated the volume and pays for whatever the fs does with it.
func statfsUsedBytes(path string) int64 {
	var st syscall.Statfs_t
	if err := syscall.Statfs(path, &st); err != nil {
		return 0
	}
	used := int64(st.Blocks-st.Bavail) * int64(st.Bsize)
	if used < 0 {
		return 0
	}
	return used
}
