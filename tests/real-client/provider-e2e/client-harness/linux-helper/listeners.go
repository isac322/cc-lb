package main

import (
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"
)

// ownedListener is an observation of a TCP LISTEN socket which is owned by
// the consumer process or one of its descendants. The timestamps are elapsed
// milliseconds from tracker creation, not wall-clock timestamps.
type ownedListener struct {
	OwnerPID          int    `json:"owner_pid"`
	OwnerProcessGroup int    `json:"owner_process_group"`
	Address           string `json:"address"`
	Port              uint16 `json:"port"`
	StartTicks        uint64 `json:"start_ticks"`
	NetworkNamespace  string `json:"network_namespace"`
	FirstSeenMS       int64  `json:"first_seen_ms"`
	LastSeenMS        int64  `json:"last_seen_ms"`

	inode string
	key   string
}

type listenerProcFS interface {
	readFile(string) ([]byte, error)
	readDir(string) ([]string, error)
	readLink(string) (string, error)
}

type osListenerProcFS struct{}

func (osListenerProcFS) readFile(path string) ([]byte, error) { return os.ReadFile(path) }

func (osListenerProcFS) readDir(path string) ([]string, error) {
	entries, err := os.ReadDir(path)
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		names = append(names, entry.Name())
	}
	sort.Strings(names)
	return names, nil
}

func (osListenerProcFS) readLink(path string) (string, error) { return os.Readlink(path) }

type listenerProcStat struct {
	pid        int
	ppid       int
	processGrp int
	startTicks uint64
}

type listenerNetEntry struct {
	inode string
	addr  string
	port  uint16
	ns    string
}

type listenerCandidate struct {
	listenerNetEntry
	owner listenerProcStat
	key   string
}

type listenerSnapshot struct {
	rootPresent bool
	all         map[int]listenerProcStat
	descendants map[int]listenerProcStat
	listeners   []listenerCandidate
}

type listenerTracker struct {
	pid            int
	helperPID      int
	procRoot       string
	fs             listenerProcFS
	now            func() time.Time
	sleep          func(time.Duration)
	startedAt      time.Time
	rootStart      uint64
	rootProcessGrp int
	rootNS         string

	processStart map[int]uint64
	observed     map[string]ownedListener
	seen         bool
	closed       bool
}

// newListenerTracker starts a Linux-only, evidence-based listener tracker.
// It intentionally reads only process metadata, network namespace links, file
// descriptor links, and TCP kernel tables. It does not inspect environments or
// user files.
func newListenerTracker(pid int) (*listenerTracker, error) {
	if runtime.GOOS != "linux" {
		return nil, errors.New("listener evidence is unsupported on non-Linux Unix")
	}
	return newListenerTrackerWithFS(pid, "/proc", osListenerProcFS{}, time.Now)
}

// newListenerTrackerWithFS is the deterministic proc-data seam used by unit
// tests. Its Linux semantics come from the supplied proc data, so construction
// is independent of the host operating system.
func newListenerTrackerWithFS(pid int, procRoot string, fs listenerProcFS, now func() time.Time) (*listenerTracker, error) {
	if pid <= 0 {
		return nil, errors.New("invalid listener root pid")
	}
	if procRoot == "" || fs == nil {
		return nil, errors.New("invalid listener proc filesystem")
	}
	if now == nil {
		now = time.Now
	}
	stat, err := readListenerProcStat(fs, filepath.Join(procRoot, strconv.Itoa(pid), "stat"))
	if err != nil {
		return nil, fmt.Errorf("inspect root process: %w", err)
	}
	if stat.pid != pid {
		return nil, errors.New("inspect root process: /proc stat pid mismatch")
	}
	ns, err := fs.readLink(filepath.Join(procRoot, strconv.Itoa(pid), "ns", "net"))
	if err != nil {
		return nil, fmt.Errorf("inspect root network namespace: %w", err)
	}
	started := now()
	return &listenerTracker{
		pid:            pid,
		helperPID:      os.Getpid(),
		procRoot:       procRoot,
		fs:             fs,
		now:            now,
		sleep:          time.Sleep,
		startedAt:      started,
		rootStart:      stat.startTicks,
		rootProcessGrp: stat.processGrp,
		rootNS:         ns,
		processStart:   map[int]uint64{pid: stat.startTicks},
		observed:       make(map[string]ownedListener),
	}, nil
}

// Observe records one non-empty, ownership-backed TCP listener snapshot.
// Observation is sampling evidence; it does not claim continuous enforcement.
func (t *listenerTracker) Observe() error {
	if t == nil {
		return errors.New("nil listener tracker")
	}
	if t.closed {
		return errors.New("listener tracker is closed")
	}
	snapshot, err := t.inspect(false)
	if err != nil {
		return err
	}
	if len(snapshot.listeners) == 0 {
		return errors.New("listener observation is empty")
	}
	t.record(snapshot)
	return nil
}

// CloseEvidence waits for the root process and every observed descendant to
// exit, then boundedly verifies that every observed socket is absent from the
// kernel TCP LISTEN tables. An empty initial observation is never accepted.
func (t *listenerTracker) CloseEvidence(timeout time.Duration) ([]ownedListener, error) {
	if t == nil {
		return nil, errors.New("nil listener tracker")
	}
	if timeout <= 0 {
		return nil, errors.New("listener closure timeout must be positive")
	}
	if !t.seen || len(t.observed) == 0 {
		return nil, errors.New("cannot prove closure without a non-empty observation")
	}
	if t.closed {
		return t.observedListeners(), nil
	}

	deadline := t.now().Add(timeout)
	for attempts := 0; ; attempts++ {
		snapshot, err := t.inspect(true)
		if err != nil {
			return nil, err
		}
		for _, current := range snapshot.listeners {
			if _, observed := t.observed[current.key]; !observed {
				return nil, fmt.Errorf("listener appeared without prior ownership evidence: %s", current.key)
			}
		}
		if !snapshot.rootPresent && len(snapshot.descendants) == 0 && trackedProcessesGone(t.processStart, snapshot.all) && len(snapshot.listeners) == 0 {
			t.closed = true
			return t.observedListeners(), nil
		}
		if !t.now().Before(deadline) {
			return nil, errors.New("listener closure evidence timed out")
		}
		if attempts >= 10000 {
			return nil, errors.New("listener closure evidence exceeded bounded observations")
		}
		remaining := deadline.Sub(t.now())
		if remaining > 10*time.Millisecond {
			remaining = 10 * time.Millisecond
		}
		before := t.now()
		t.sleep(remaining)
		if !t.now().After(before) && attempts >= 1 {
			return nil, errors.New("listener closure evidence timed out")
		}
	}
}

func (t *listenerTracker) record(snapshot listenerSnapshot) {
	nowMS := t.elapsedMS()
	for _, candidate := range snapshot.listeners {
		entry, exists := t.observed[candidate.key]
		if !exists {
			entry = ownedListener{
				OwnerPID:          candidate.owner.pid,
				OwnerProcessGroup: candidate.owner.processGrp,
				Address:           candidate.addr,
				Port:              candidate.port,
				StartTicks:        candidate.owner.startTicks,
				NetworkNamespace:  candidate.ns,
				FirstSeenMS:       nowMS,
				LastSeenMS:        nowMS,
				inode:             candidate.inode,
				key:               candidate.key,
			}
		} else {
			entry.OwnerPID = candidate.owner.pid
			entry.OwnerProcessGroup = candidate.owner.processGrp
			entry.StartTicks = candidate.owner.startTicks
			entry.LastSeenMS = nowMS
		}
		t.observed[candidate.key] = entry
	}
	for pid, process := range snapshot.descendants {
		t.processStart[pid] = process.startTicks
	}
	t.seen = true
}

func (t *listenerTracker) observedListeners() []ownedListener {
	out := make([]ownedListener, 0, len(t.observed))
	for _, entry := range t.observed {
		entry.inode = ""
		entry.key = ""
		out = append(out, entry)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Address != out[j].Address {
			return out[i].Address < out[j].Address
		}
		if out[i].Port != out[j].Port {
			return out[i].Port < out[j].Port
		}
		return out[i].OwnerPID < out[j].OwnerPID
	})
	return out
}

func (t *listenerTracker) elapsedMS() int64 {
	elapsed := t.now().Sub(t.startedAt).Milliseconds()
	if elapsed < 0 {
		return 0
	}
	return elapsed
}

func (t *listenerTracker) inspect(allowEmpty bool) (listenerSnapshot, error) {
	all, err := t.readAllStats()
	if err != nil {
		return listenerSnapshot{}, err
	}
	root, rootPresent := all[t.pid]
	if rootPresent {
		if root.startTicks != t.rootStart || root.processGrp != t.rootProcessGrp {
			return listenerSnapshot{}, errors.New("listener root pid was reused or changed process group")
		}
		rootNS, err := t.fs.readLink(filepath.Join(t.procRoot, strconv.Itoa(t.pid), "ns", "net"))
		if err != nil {
			return listenerSnapshot{}, fmt.Errorf("inspect root network namespace: %w", err)
		}
		if rootNS != t.rootNS {
			return listenerSnapshot{}, errors.New("listener root network namespace changed")
		}
	}

	descendants := make(map[int]listenerProcStat)
	if rootPresent {
		descendants[t.pid] = root
		for pid, process := range all {
			if pid != t.pid && isListenerDescendant(pid, all, t.pid) {
				descendants[pid] = process
			}
		}
	}
	for pid, priorStart := range t.processStart {
		if current, ok := all[pid]; ok && current.startTicks != priorStart {
			return listenerSnapshot{}, fmt.Errorf("listener process pid %d was reused", pid)
		}
	}

	owners := make(map[string][]listenerProcStat)
	nsProcesses := make(map[string]int)
	for pid, process := range descendants {
		ns, err := t.fs.readLink(filepath.Join(t.procRoot, strconv.Itoa(pid), "ns", "net"))
		if err != nil {
			if isListenerNotExist(err) {
				continue
			}
			return listenerSnapshot{}, fmt.Errorf("inspect process %d network namespace: %w", pid, err)
		}
		nsProcesses[ns] = pid
		inodes, err := t.readSocketInodes(pid)
		if err != nil {
			if isListenerNotExist(err) {
				continue
			}
			return listenerSnapshot{}, err
		}
		for _, inode := range inodes {
			owners[listenerSocketKey(ns, inode)] = append(owners[listenerSocketKey(ns, inode)], process)
		}
	}
	if len(nsProcesses) == 0 {
		for pid := range all {
			ns, nsErr := t.fs.readLink(filepath.Join(t.procRoot, strconv.Itoa(pid), "ns", "net"))
			if nsErr != nil {
				if isListenerNotExist(nsErr) {
					continue
				}
				return listenerSnapshot{}, fmt.Errorf("inspect process %d network namespace: %w", pid, nsErr)
			}
			if ns == t.rootNS {
				nsProcesses[ns] = pid
				break
			}
		}
	}
	entries, err := t.readNetworkTables(nsProcesses)
	if err != nil {
		return listenerSnapshot{}, err
	}
	listeners, unresolved, err := t.resolveListeners(entries, owners)
	if err != nil {
		return listenerSnapshot{}, err
	}
	auditEntries := make([]listenerNetEntry, 0, len(entries))
	for _, entry := range entries {
		if entry.port != 18080 {
			auditEntries = append(auditEntries, entry)
		}
	}
	if len(auditEntries) > 0 {
		if err := t.rejectForeignListeners(auditEntries, all, descendants); err != nil {
			return listenerSnapshot{}, err
		}
	}
	if len(unresolved) > 0 {
		return listenerSnapshot{}, errors.New("listener inode has no process ownership evidence")
	}
	if len(listeners) == 0 && !allowEmpty {
		return listenerSnapshot{}, errors.New("listener observation is empty")
	}
	return listenerSnapshot{rootPresent: rootPresent, all: all, descendants: descendants, listeners: listeners}, nil
}

func (t *listenerTracker) readAllStats() (map[int]listenerProcStat, error) {
	names, err := t.fs.readDir(t.procRoot)
	if err != nil {
		return nil, fmt.Errorf("inspect proc process list: %w", err)
	}
	all := make(map[int]listenerProcStat)
	for _, name := range names {
		pid, err := strconv.Atoi(name)
		if err != nil || pid <= 0 {
			continue
		}
		stat, err := readListenerProcStat(t.fs, filepath.Join(t.procRoot, name, "stat"))
		if err != nil {
			if isListenerNotExist(err) {
				continue
			}
			return nil, fmt.Errorf("inspect process %d stat: %w", pid, err)
		}
		if stat.pid != pid {
			return nil, fmt.Errorf("inspect process %d stat: /proc stat pid mismatch", pid)
		}
		all[pid] = stat
	}
	if _, ok := all[t.pid]; !ok {
		stat, statErr := readListenerProcStat(t.fs, filepath.Join(t.procRoot, strconv.Itoa(t.pid), "stat"))
		if statErr == nil {
			if stat.pid != t.pid {
				return nil, errors.New("inspect root process stat: /proc stat pid mismatch")
			}
			all[t.pid] = stat
		} else if !isListenerNotExist(statErr) {
			return nil, fmt.Errorf("inspect root process stat: %w", statErr)
		}
	}
	return all, nil
}

func (t *listenerTracker) readSocketInodes(pid int) ([]string, error) {
	names, err := t.fs.readDir(filepath.Join(t.procRoot, strconv.Itoa(pid), "fd"))
	if err != nil {
		return nil, err
	}
	inodes := make([]string, 0)
	for _, name := range names {
		target, err := t.fs.readLink(filepath.Join(t.procRoot, strconv.Itoa(pid), "fd", name))
		if err != nil {
			if isListenerNotExist(err) {
				continue
			}
			return nil, fmt.Errorf("inspect process %d fd %s: %w", pid, name, err)
		}
		if inode, ok := listenerSocketInode(target); ok {
			inodes = append(inodes, inode)
		}
	}
	return inodes, nil
}

func (t *listenerTracker) readNetworkTables(nsProcesses map[string]int) ([]listenerNetEntry, error) {
	paths := make(map[string]int)
	for ns, pid := range nsProcesses {
		paths[ns] = pid
	}
	entries := make([]listenerNetEntry, 0)
	for ns, pid := range paths {
		base := t.procRoot
		if pid > 0 {
			base = filepath.Join(base, strconv.Itoa(pid))
		}
		readTable := false
		for _, table := range []struct {
			name string
			v6   bool
		}{
			{name: "tcp", v6: false},
			{name: "tcp6", v6: true},
		} {
			path := filepath.Join(base, "net", table.name)
			data, err := t.fs.readFile(path)
			if err != nil {
				if isListenerNotExist(err) {
					continue
				}
				return nil, fmt.Errorf("inspect %s: %w", path, err)
			}
			readTable = true
			parsed, err := parseListenerNetTable(data, table.v6)
			if err != nil {
				return nil, fmt.Errorf("inspect %s: %w", path, err)
			}
			for _, entry := range parsed {
				entry.ns = ns
				entries = append(entries, entry)
			}
		}
		if !readTable {
			return nil, fmt.Errorf("inspect network namespace %s: TCP tables unavailable", ns)
		}
	}
	return entries, nil
}

func (t *listenerTracker) resolveListeners(entries []listenerNetEntry, owners map[string][]listenerProcStat) ([]listenerCandidate, []listenerNetEntry, error) {
	byKey := make(map[string]listenerCandidate)
	unresolved := make([]listenerNetEntry, 0)
	for _, entry := range entries {
		if entry.port == 18080 {
			continue
		}
		key := listenerSocketKey(entry.ns, entry.inode)
		processes := owners[key]
		filtered := make([]listenerProcStat, 0, len(processes))
		for _, process := range processes {
			if process.pid != t.helperPID {
				filtered = append(filtered, process)
			}
		}
		processes = filtered
		if len(processes) == 0 {
			if len(owners[key]) > 0 {
				continue
			}
			unresolved = append(unresolved, entry)
			continue
		}
		if entry.addr != "127.0.0.1" && entry.addr != "::1" {
			return nil, nil, fmt.Errorf("owned listener address is not numeric loopback: %s", entry.addr)
		}
		owner := processes[0]
		for _, process := range processes[1:] {
			if process.pid < owner.pid {
				owner = process
			}
		}
		listenerKey := fmt.Sprintf("%s:%s:%s:%d", entry.ns, entry.inode, entry.addr, entry.port)
		candidate := listenerCandidate{listenerNetEntry: entry, owner: owner, key: listenerKey}
		if prior, exists := byKey[listenerKey]; !exists || candidate.owner.pid < prior.owner.pid {
			byKey[listenerKey] = candidate
		}
	}
	listeners := make([]listenerCandidate, 0, len(byKey))
	for _, candidate := range byKey {
		listeners = append(listeners, candidate)
	}
	sort.Slice(listeners, func(i, j int) bool { return listeners[i].key < listeners[j].key })
	return listeners, unresolved, nil
}

func (t *listenerTracker) rejectForeignListeners(entries []listenerNetEntry, all, descendants map[int]listenerProcStat) error {
	targets := make(map[string]listenerNetEntry)
	for _, entry := range entries {
		targets[listenerSocketKey(entry.ns, entry.inode)] = entry
	}
	found := make(map[string]bool)
	for pid := range all {
		inodes, err := t.readSocketInodes(pid)
		if err != nil {
			if isListenerNotExist(err) {
				continue
			}
			return fmt.Errorf("inspect possible listener owner %d: %w", pid, err)
		}
		if len(inodes) == 0 {
			continue
		}
		ns, err := t.fs.readLink(filepath.Join(t.procRoot, strconv.Itoa(pid), "ns", "net"))
		if err != nil {
			if isListenerNotExist(err) {
				continue
			}
			return fmt.Errorf("inspect possible listener owner %d namespace: %w", pid, err)
		}
		for _, inode := range inodes {
			key := listenerSocketKey(ns, inode)
			if _, wanted := targets[key]; !wanted {
				continue
			}
			found[key] = true
			if pid == t.helperPID {
				continue
			}
			if _, owned := descendants[pid]; !owned {
				return fmt.Errorf("listener inode %s is owned by a foreign pid %d", inode, pid)
			}
		}
	}
	for key := range targets {
		if !found[key] {
			return errors.New("listener inode has no live process owner")
		}
	}
	return nil
}

func readListenerProcStat(fs listenerProcFS, path string) (listenerProcStat, error) {
	data, err := fs.readFile(path)
	if err != nil {
		return listenerProcStat{}, err
	}
	return parseListenerProcStat(data)
}

func parseListenerProcStat(data []byte) (listenerProcStat, error) {
	text := strings.TrimSpace(string(data))
	closeParen := strings.LastIndexByte(text, ')')
	if closeParen <= 0 || closeParen+2 >= len(text) {
		return listenerProcStat{}, errors.New("malformed /proc stat")
	}
	pidEnd := strings.IndexByte(text, ' ')
	if pidEnd <= 0 {
		return listenerProcStat{}, errors.New("malformed /proc stat pid")
	}
	pid, err := strconv.Atoi(text[:pidEnd])
	if err != nil || pid <= 0 {
		return listenerProcStat{}, errors.New("malformed /proc stat pid")
	}
	fields := strings.Fields(text[closeParen+1:])
	if len(fields) <= 19 {
		return listenerProcStat{}, errors.New("malformed /proc stat fields")
	}
	ppid, err := strconv.Atoi(fields[1])
	if err != nil || ppid < 0 {
		return listenerProcStat{}, errors.New("malformed /proc stat ppid")
	}
	processGrp, err := strconv.Atoi(fields[2])
	if err != nil || processGrp <= 0 {
		return listenerProcStat{}, errors.New("malformed /proc stat process group")
	}
	start, err := strconv.ParseUint(fields[19], 10, 64)
	if err != nil || start == 0 {
		return listenerProcStat{}, errors.New("malformed /proc stat start time")
	}
	return listenerProcStat{pid: pid, ppid: ppid, processGrp: processGrp, startTicks: start}, nil
}

func isListenerDescendant(pid int, all map[int]listenerProcStat, root int) bool {
	visited := make(map[int]struct{})
	for pid != root {
		if pid <= 0 {
			return false
		}
		if _, ok := visited[pid]; ok {
			return false
		}
		visited[pid] = struct{}{}
		process, ok := all[pid]
		if !ok {
			return false
		}
		pid = process.ppid
	}
	return true
}

func trackedProcessesGone(prior map[int]uint64, all map[int]listenerProcStat) bool {
	for pid := range prior {
		if _, exists := all[pid]; exists {
			return false
		}
	}
	return true
}

// parseListenerNetTable intentionally handles only Linux TCP/TCP6 tables.
func parseListenerNetTable(data []byte, ipv6 bool) ([]listenerNetEntry, error) {
	lines := strings.Split(string(data), "\n")
	entries := make([]listenerNetEntry, 0)
	for _, line := range lines {
		fields := strings.Fields(line)
		if len(fields) == 0 || fields[0] == "sl" {
			continue
		}
		if len(fields) < 4 {
			return nil, errors.New("malformed TCP table row")
		}
		if fields[3] != "0A" {
			continue
		}
		if len(fields) < 10 {
			return nil, errors.New("malformed TCP LISTEN row")
		}
		local := strings.Split(fields[1], ":")
		if len(local) != 2 || local[0] == "" || local[1] == "" {
			return nil, errors.New("malformed TCP local address")
		}
		portValue, err := strconv.ParseUint(local[1], 16, 16)
		if err != nil || portValue == 0 {
			return nil, errors.New("malformed TCP local port")
		}
		inodeValue, err := strconv.ParseUint(fields[9], 10, 64)
		if err != nil || inodeValue == 0 {
			return nil, errors.New("malformed TCP socket inode")
		}
		address, err := decodeListenerAddress(local[0], ipv6)
		if err != nil {
			return nil, err
		}
		entries = append(entries, listenerNetEntry{inode: strconv.FormatUint(inodeValue, 10), addr: address, port: uint16(portValue)})
	}
	return entries, nil
}

func decodeListenerAddress(encoded string, ipv6 bool) (string, error) {
	if ipv6 {
		if len(encoded) != 32 {
			return "", errors.New("malformed TCP6 address")
		}
		raw, err := hex.DecodeString(encoded)
		if err != nil {
			return "", errors.New("malformed TCP6 address")
		}
		decoded := make([]byte, 16)
		for offset := 0; offset < 16; offset += 4 {
			decoded[offset] = raw[offset+3]
			decoded[offset+1] = raw[offset+2]
			decoded[offset+2] = raw[offset+1]
			decoded[offset+3] = raw[offset]
		}
		ip := net.IP(decoded)
		if !ip.Equal(net.ParseIP("::1")) {
			return ip.String(), nil
		}
		return "::1", nil
	}
	if len(encoded) != 8 {
		return "", errors.New("malformed TCP address")
	}
	raw, err := hex.DecodeString(encoded)
	if err != nil {
		return "", errors.New("malformed TCP address")
	}
	decoded := []byte{raw[3], raw[2], raw[1], raw[0]}
	return net.IP(decoded).String(), nil
}

func listenerSocketInode(target string) (string, bool) {
	if !strings.HasPrefix(target, "socket:[") || !strings.HasSuffix(target, "]") {
		return "", false
	}
	inode := strings.TrimSuffix(strings.TrimPrefix(target, "socket:["), "]")
	if inode == "" {
		return "", false
	}
	if _, err := strconv.ParseUint(inode, 10, 64); err != nil {
		return "", false
	}
	return inode, true
}

func listenerSocketKey(namespace, inode string) string { return namespace + "\x00" + inode }

func isListenerNotExist(err error) bool { return errors.Is(err, os.ErrNotExist) }
