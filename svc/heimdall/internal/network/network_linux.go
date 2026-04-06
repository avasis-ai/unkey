//go:build linux

package network

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/cilium/ebpf"
	"github.com/cilium/ebpf/link"
	"github.com/cilium/ebpf/rlimit"
	containerd "github.com/containerd/containerd/v2/client"
	"k8s.io/apimachinery/pkg/types"
)

// linuxReader is the real eBPF-backed implementation of network.Reader.
// Sandbox netns resolution lives in sandbox_linux.go; veth-ifindex discovery
// lives in veth_linux.go. This file holds just the Reader interface methods.
type linuxReader struct {
	objs    bpfObjects
	cd      *containerd.Client // sandbox-container lookups for netns resolution
	mu      sync.Mutex
	links   map[types.UID]podLinks // pod uid → its two tc attach handles
	ifindex map[types.UID]uint32   // pod uid → host-side veth ifindex (BPF map key)
}

type podLinks struct {
	ingress link.Link
	egress  link.Link
}

// newReader loads the eBPF program once per process and dials containerd
// at criSocket. Loads need RLIMIT_MEMLOCK lifted (rlimit.RemoveMemlock)
// and CAP_BPF on kernels >= 5.8 (or CAP_SYS_ADMIN on older). TCX attach
// additionally needs CAP_NET_ADMIN. The DaemonSet manifest grants all
// three. A failing containerd dial is fatal: without it, we can't resolve
// the CNI netns for any pod under gVisor (no IP on eth0 to match on) and
// a silently-broken collector would undercharge every pod.
func newReader(criSocket string) (Reader, error) {
	if err := rlimit.RemoveMemlock(); err != nil {
		return nil, fmt.Errorf("rlimit.RemoveMemlock: %w", err)
	}

	if criSocket == "" {
		return nil, errors.New("cri socket required for sandbox netns resolution")
	}

	cd, err := containerd.New(criSocket, containerd.WithDefaultNamespace("k8s.io"))
	if err != nil {
		return nil, fmt.Errorf("dial containerd at %s: %w", criSocket, err)
	}

	r := &linuxReader{
		objs:    bpfObjects{}, //nolint:exhaustruct // populated by loadBpfObjects below
		cd:      cd,
		mu:      sync.Mutex{},
		links:   make(map[types.UID]podLinks),
		ifindex: make(map[types.UID]uint32),
	}
	if err := loadBpfObjects(&r.objs, nil); err != nil {
		_ = cd.Close()
		return nil, fmt.Errorf("loadBpfObjects: %w", err)
	}
	return r, nil
}

// Attach resolves the pod's CNI netns via containerd and hooks the two
// tc programs onto the host-side veth peer of eth0 in that netns.
// Idempotent - re-attaching a pod that's already attached is a no-op.
func (r *linuxReader) Attach(uid types.UID) error {
	r.mu.Lock()
	if _, ok := r.links[uid]; ok {
		r.mu.Unlock()
		return nil
	}
	r.mu.Unlock()

	// Short per-call timeout so a wedged containerd gRPC never holds up the
	// 5s collection tick.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	netnsPath, err := r.sandboxNetnsPath(ctx, string(uid))
	if err != nil {
		return fmt.Errorf("resolve sandbox netns: %w", err)
	}

	ifindex, err := hostVethIfindex(netnsPath)
	if err != nil {
		return fmt.Errorf("resolve host veth ifindex (netns=%s): %w", netnsPath, err)
	}

	// Head anchor so we run upstream of Cilium's `cil_from_container` on
	// tc-ingress of the host-side veth. Cilium returns TC_ACT_REDIRECT for
	// most pod-outbound packets (direct-routing to cilium_host), which
	// terminates the TCX chain - a tail-anchored observer would never see
	// those packets.
	//
	// Running first is safe *only because* our BPF programs return
	// TC_ACT_UNSPEC (TCX_NEXT), which is non-terminating: Cilium still gets
	// to run next and do its ClusterIP→pod-IP translation. Returning
	// TC_ACT_OK here instead would be TCX_PASS (accept + terminate) and
	// kill Cilium's translation, which breaks DNS for gVisor pods (gVisor
	// bypasses Cilium's socket-LB so the veth's tc-BPF is the only place
	// service IPs get rewritten). See bpf/network.bpf.c for the program.
	//
	// Retina's packetparser plugin uses this exact pattern:
	// microsoft/retina pkg/plugin/packetparser/packetparser_linux.go:600-604.
	egress, err := link.AttachTCX(link.TCXOptions{ //nolint:exhaustruct // Flags/ExpectedRevision optional
		Interface: int(ifindex),
		Program:   r.objs.CountEgress,
		Attach:    ebpf.AttachTCXIngress, // pod egress = packets arriving on the host veth's ingress
		Anchor:    link.Head(),
	})
	if err != nil {
		return fmt.Errorf("attach tcx ingress (pod egress): %w", err)
	}

	ingressLink, err := link.AttachTCX(link.TCXOptions{ //nolint:exhaustruct
		Interface: int(ifindex),
		Program:   r.objs.CountIngress,
		Attach:    ebpf.AttachTCXEgress, // pod ingress = packets leaving the host veth toward the pod
		Anchor:    link.Head(),
	})
	if err != nil {
		_ = egress.Close()
		return fmt.Errorf("attach tcx egress (pod ingress): %w", err)
	}

	r.mu.Lock()

	// Re-check under the lock - another goroutine may have raced us to attach.
	if _, ok := r.links[uid]; ok {
		_ = egress.Close()
		_ = ingressLink.Close()
		r.mu.Unlock()
		return nil
	}

	r.links[uid] = podLinks{ingress: ingressLink, egress: egress}
	r.ifindex[uid] = ifindex
	r.mu.Unlock()
	return nil
}

// Detach closes both tc links and drops the pod's BPF map entry. TCX links
// auto-detach on close, and the veth usually disappears at pod teardown
// anyway (taking the attachment with it), but doing it eagerly keeps the
// map sparse and frees the LRU slot immediately.
func (r *linuxReader) Detach(uid types.UID) {
	r.mu.Lock()
	defer r.mu.Unlock()
	links, ok := r.links[uid]
	if !ok {
		return
	}

	_ = links.ingress.Close()
	_ = links.egress.Close()
	delete(r.links, uid)

	if ifindex, ok := r.ifindex[uid]; ok {
		_ = r.objs.PodCounters.Delete(ifindex)
		delete(r.ifindex, uid)
	}
}

// Read returns the current cumulative byte counters for the pod. One map
// lookup by veth ifindex.
//
// Returns zeros (no error) when the pod isn't attached or hasn't seen any
// packets yet - indistinguishable from all-zero.
func (r *linuxReader) Read(uid types.UID) (Counters, error) {
	r.mu.Lock()
	ifindex, ok := r.ifindex[uid]
	r.mu.Unlock()
	if !ok {
		return zeroCounters, nil
	}

	var c bpfCounters
	if err := r.objs.PodCounters.Lookup(ifindex, &c); err != nil {
		if errors.Is(err, ebpf.ErrKeyNotExist) {
			return zeroCounters, nil
		}
		return zeroCounters, fmt.Errorf("map lookup ifindex=%d: %w", ifindex, err)
	}

	return Counters{
		EgressPublicBytes:   int64(c.EgressPublic),
		EgressPrivateBytes:  int64(c.EgressPrivate),
		IngressPublicBytes:  int64(c.IngressPublic),
		IngressPrivateBytes: int64(c.IngressPrivate),
	}, nil
}

func (r *linuxReader) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	for uid, links := range r.links {
		_ = links.ingress.Close()
		_ = links.egress.Close()
		delete(r.links, uid)
	}

	r.ifindex = map[types.UID]uint32{}
	_ = r.cd.Close()

	return r.objs.Close()
}
