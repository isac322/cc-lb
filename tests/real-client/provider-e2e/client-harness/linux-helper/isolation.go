package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"
)

const (
	isolationBlockedCode = "QHARNESS_LINUX_ISOLATION_BLOCKED"
	clientArtifactPath   = "/opt/agy"
	clientName           = "agy"
	clientVersion        = "1.5.9"
	clientPlatform       = "linux"
	clientArch           = "arm64"
	clientLibc           = "musl"
	clientSHA256         = "d81a2547df4d29e391725219c1908056885abc6c73fd23819553f622ac9b098d"
	helperPort           = 18080
	maxVersionOutput     = 64 << 10
	maxArtifactBytes     = 256 << 20
)

func isolationBlocked() error { return errors.New(isolationBlockedCode) }

type statusObservation struct {
	uid              int
	capabilitiesZero bool
	noNewPrivileges  bool
}

// parseStatus parses the bounded, line-oriented /proc/self/status format. It
// deliberately requires every field used by the isolation proof; an omitted
// field is not interpreted as a safe default.
func parseStatus(data []byte) (statusObservation, error) {
	var result statusObservation
	var uidSeen, capSeen, nnpSeen bool
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		switch strings.TrimSuffix(fields[0], ":") {
		case "Uid":
			if len(fields) != 5 || uidSeen {
				return result, isolationBlocked()
			}
			var values [4]int
			for i, value := range fields[1:] {
				n, err := strconv.Atoi(value)
				if err != nil || n < 0 {
					return result, isolationBlocked()
				}
				values[i] = n
			}
			for _, value := range values[1:] {
				if value != values[0] {
					return result, isolationBlocked()
				}
			}
			result.uid = values[0]
			uidSeen = true
		case "CapEff":
			if len(fields) != 2 || capSeen || len(fields[1]) != 16 {
				return result, isolationBlocked()
			}
			value, err := strconv.ParseUint(fields[1], 16, 64)
			if err != nil {
				return result, isolationBlocked()
			}
			result.capabilitiesZero = value == 0
			capSeen = true
		case "NoNewPrivs":
			if len(fields) != 2 || nnpSeen || (fields[1] != "0" && fields[1] != "1") {
				return result, isolationBlocked()
			}
			result.noNewPrivileges = fields[1] == "1"
			nnpSeen = true
		}
	}
	if !uidSeen || !capSeen || !nnpSeen || result.uid != 65534 || !result.capabilitiesZero || !result.noNewPrivileges {
		return result, isolationBlocked()
	}
	return result, nil
}

type routeObservation struct {
	defaultRoute     bool
	nonLoopbackRoute bool
}

func parseIPv4Routes(data []byte) (routeObservation, error) {
	var result routeObservation
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if strings.Join(strings.Fields(lines[0]), " ") != "Iface Destination Gateway Flags RefCnt Use Metric Mask MTU Window IRTT" {
		return result, isolationBlocked()
	}
	for _, line := range lines[1:] {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) != 11 || len(fields[1]) != 8 || len(fields[2]) != 8 || len(fields[7]) != 8 {
			return result, isolationBlocked()
		}
		if _, err := strconv.ParseUint(fields[1], 16, 32); err != nil {
			return result, isolationBlocked()
		}
		if _, err := strconv.ParseUint(fields[7], 16, 32); err != nil {
			return result, isolationBlocked()
		}
		if fields[0] == "" {
			return result, isolationBlocked()
		}
		if fields[1] == "00000000" && fields[7] == "00000000" {
			result.defaultRoute = true
		}
		if fields[0] != "lo" {
			result.nonLoopbackRoute = true
		}
	}
	return result, nil
}

func parseIPv6Routes(data []byte) (routeObservation, error) {
	var result routeObservation
	text := strings.TrimSpace(string(data))
	if text == "" {
		return result, nil
	}
	for _, line := range strings.Split(text, "\n") {
		fields := strings.Fields(line)
		if len(fields) < 10 {
			return result, isolationBlocked()
		}
		if len(fields[0]) != 32 || len(fields[2]) != 32 || len(fields[4]) != 32 {
			return result, isolationBlocked()
		}
		prefix, err := strconv.ParseUint(fields[1], 16, 8)
		if err != nil || prefix > 128 {
			return result, isolationBlocked()
		}
		flags, err := strconv.ParseUint(fields[8], 16, 32)
		if err != nil {
			return result, isolationBlocked()
		}
		for _, value := range []string{fields[0], fields[2], fields[4]} {
			if _, err := hex.DecodeString(value); err != nil {
				return result, isolationBlocked()
			}
		}
		if fields[9] == "" {
			return result, isolationBlocked()
		}
		// Linux publishes synthetic unreachable loopback entries for ::/0.
		// They are rejection entries, not a usable default forwarding route.
		if strings.Trim(fields[0], "0") == "" && prefix == 0 && flags&0x200 == 0 {
			result.defaultRoute = true
		}
		if fields[9] != "lo" {
			result.nonLoopbackRoute = true
		}
	}
	return result, nil
}

func observeRoutes() (bool, error) {
	v4, err := os.ReadFile("/proc/net/route")
	if err != nil {
		return false, isolationBlocked()
	}
	v6, err := os.ReadFile("/proc/net/ipv6_route")
	if err != nil {
		return false, isolationBlocked()
	}
	r4, err := parseIPv4Routes(v4)
	if err != nil {
		return false, isolationBlocked()
	}
	r6, err := parseIPv6Routes(v6)
	if err != nil {
		return false, isolationBlocked()
	}
	return !r4.defaultRoute && !r4.nonLoopbackRoute && !r6.defaultRoute && !r6.nonLoopbackRoute, nil
}

func observeLoopbackOnly() (bool, error) {
	interfaces, err := net.Interfaces()
	if err != nil || len(interfaces) == 0 {
		return false, isolationBlocked()
	}
	for _, iface := range interfaces {
		if iface.Name != "lo" || iface.Flags&net.FlagLoopback == 0 {
			return false, nil
		}
	}
	return len(interfaces) == 1, nil
}

func isIsolationDeniedError(err error) bool {
	var networkError net.Error
	if errors.As(err, &networkError) && networkError.Timeout() {
		return false
	}
	return errors.Is(err, syscall.ENETUNREACH) || errors.Is(err, syscall.EHOSTUNREACH) || errors.Is(err, syscall.EPERM) || errors.Is(err, syscall.EACCES)
}

func isConnectionRefusedError(err error) bool {
	var networkError net.Error
	return !(errors.As(err, &networkError) && networkError.Timeout()) && errors.Is(err, syscall.ECONNREFUSED)
}

func boundedDeniedDial(network, address string) (bool, error) {
	dialer := net.Dialer{Timeout: 750 * time.Millisecond}
	connection, err := dialer.Dial(network, address)
	if err == nil {
		_ = connection.Close()
		return false, isolationBlocked()
	}
	if !isIsolationDeniedError(err) {
		return false, isolationBlocked()
	}
	return true, nil
}

func boundedHostCanaryDial(port int) (bool, error) {
	if port < 1 || port > 65535 || port == helperPort {
		return false, isolationBlocked()
	}
	dialer := net.Dialer{Timeout: 750 * time.Millisecond}
	connection, err := dialer.Dial("tcp4", net.JoinHostPort("127.0.0.1", strconv.Itoa(port)))
	if err == nil {
		_ = connection.Close()
		return false, isolationBlocked()
	}
	if !isConnectionRefusedError(err) {
		return false, isolationBlocked()
	}
	return true, nil
}

type mountRecord struct {
	root        string
	mountPoint  string
	options     map[string]string
	optional    []string
	fsType      string
	source      string
	superOption map[string]string
}

func decodeMountInfoPath(value string) (string, error) {
	var builder strings.Builder
	for i := 0; i < len(value); i++ {
		if value[i] != '\\' {
			builder.WriteByte(value[i])
			continue
		}
		if i+3 >= len(value) || value[i+1] < '0' || value[i+1] > '7' || value[i+2] < '0' || value[i+2] > '7' || value[i+3] < '0' || value[i+3] > '7' {
			return "", isolationBlocked()
		}
		escape := value[i : i+4]
		if escape != "\\040" && escape != "\\011" && escape != "\\012" && escape != "\\134" {
			return "", isolationBlocked()
		}
		decoded := (value[i+1]-'0')*64 + (value[i+2]-'0')*8 + value[i+3] - '0'
		builder.WriteByte(decoded)
		i += 3
	}
	return builder.String(), nil
}

func parseMountOptions(value string) map[string]string {
	result := make(map[string]string)
	for _, option := range strings.Split(value, ",") {
		if option == "" {
			continue
		}
		parts := strings.SplitN(option, "=", 2)
		if len(parts) == 1 {
			result[parts[0]] = ""
		} else {
			result[parts[0]] = parts[1]
		}
	}
	return result
}

func parseMountInfo(data []byte) ([]mountRecord, error) {
	text := strings.TrimSpace(string(data))
	if text == "" {
		return nil, isolationBlocked()
	}
	result := make([]mountRecord, 0)
	for _, line := range strings.Split(text, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		parts := strings.Split(line, " - ")
		if len(parts) != 2 {
			return nil, isolationBlocked()
		}
		left := strings.Fields(parts[0])
		right := strings.Fields(parts[1])
		if len(left) < 6 || len(right) != 3 {
			return nil, isolationBlocked()
		}
		for _, identifier := range left[:2] {
			if _, err := strconv.ParseUint(identifier, 10, 64); err != nil {
				return nil, isolationBlocked()
			}
		}
		root, err := decodeMountInfoPath(left[3])
		if err != nil {
			return nil, isolationBlocked()
		}
		mountPoint, err := decodeMountInfoPath(left[4])
		if err != nil {
			return nil, isolationBlocked()
		}
		source, err := decodeMountInfoPath(right[1])
		if err != nil {
			return nil, isolationBlocked()
		}
		if !filepath.IsAbs(root) || !filepath.IsAbs(mountPoint) || filepath.Clean(mountPoint) != mountPoint || strings.ContainsAny(root+mountPoint+source, "\x00\r\n") {
			return nil, isolationBlocked()
		}
		result = append(result, mountRecord{root: root, mountPoint: mountPoint, options: parseMountOptions(left[5]), optional: append([]string(nil), left[6:]...), fsType: right[0], source: source, superOption: parseMountOptions(right[2])})
	}
	if len(result) == 0 {
		return nil, isolationBlocked()
	}
	return result, nil
}

func hasPropagation(record mountRecord) bool {
	for _, value := range record.optional {
		if strings.HasPrefix(value, "shared:") || strings.HasPrefix(value, "master:") || strings.HasPrefix(value, "propagate_from:") {
			return true
		}
	}
	return false
}

func hasOption(record mountRecord, key string) bool {
	_, ok := record.options[key]
	return ok
}

func requiredTmpfsFlags(record mountRecord) bool {
	if record.fsType != "tmpfs" || hasPropagation(record) || hasOption(record, "ro") || !hasOption(record, "rw") || !hasOption(record, "nosuid") || !hasOption(record, "nodev") || !hasOption(record, "noexec") {
		return false
	}
	if record.superOption["uid"] != "65534" || record.superOption["gid"] != "65534" {
		return false
	}
	mode := record.superOption["mode"]
	if mode != "700" && mode != "0700" {
		return false
	}
	size := record.superOption["size"]
	if size != "67108864" && size != "65536k" && size != "64m" {
		return false
	}
	return true
}

func suspiciousMount(record mountRecord) bool {
	value := strings.ToLower(record.mountPoint + " " + record.source)
	for _, marker := range []string{"docker.sock", "/run/dbus", "/var/run/dbus", "pulse", "credential", "/run/secrets", "/var/run/secrets", "/dev/snd", "/dev/dri", "/dev/video", "/dev/kvm", "/dev/fuse", "/dev/bus", "audio"} {
		if strings.Contains(value, marker) {
			return true
		}
	}
	return false
}

func verifyMountRecords(records []mountRecord) bool {
	const agyMount = "/opt/agy"
	const helperMount = "/opt/qharness/helper"
	const fixtureMount = "/opt/qharness/fixture.json"
	expectedBinds := map[string]bool{agyMount: false, helperMount: false, fixtureMount: false}
	expectedTmpfs := map[string]bool{"/home/qharness": false, "/tmp": false, "/var/lib/postgresql": false}
	rootReadOnly := false
	for _, record := range records {
		if suspiciousMount(record) {
			return false
		}
		if record.mountPoint == "/" && hasOption(record, "ro") {
			rootReadOnly = true
		}
		if _, ok := expectedBinds[record.mountPoint]; ok {
			if expectedBinds[record.mountPoint] || hasPropagation(record) || !hasOption(record, "ro") || hasOption(record, "rw") {
				return false
			}
			expectedBinds[record.mountPoint] = true
			continue
		}
		if _, ok := expectedTmpfs[record.mountPoint]; ok {
			if expectedTmpfs[record.mountPoint] || !requiredTmpfsFlags(record) {
				return false
			}
			expectedTmpfs[record.mountPoint] = true
			continue
		}
		if !standardContainerMount(record) {
			return false
		}
	}
	if !rootReadOnly {
		return false
	}
	for _, present := range expectedBinds {
		if !present {
			return false
		}
	}
	for _, present := range expectedTmpfs {
		if !present {
			return false
		}
	}
	return true
}

// Standard runtime mounts are kernel pseudo-filesystems, Docker's three
// readonly network files, and its documented proc masks. The host-side
// inspect independently binds the exact three artifact source identities.
func standardContainerMount(record mountRecord) bool {
	switch record.mountPoint {
	case "/":
		return hasOption(record, "ro") && !hasOption(record, "rw")
	case "/etc/hostname", "/etc/hosts", "/etc/resolv.conf":
		return hasOption(record, "ro") && !hasOption(record, "rw")
	case "/proc", "/proc/bus", "/proc/fs", "/proc/irq", "/proc/sys", "/proc/sysrq-trigger":
		return record.fsType == "proc"
	case "/dev":
		return record.fsType == "tmpfs"
	case "/dev/pts":
		return record.fsType == "devpts"
	case "/dev/shm":
		return record.fsType == "tmpfs"
	case "/dev/mqueue":
		return record.fsType == "mqueue"
	case "/sys":
		return record.fsType == "sysfs" && hasOption(record, "ro")
	case "/sys/fs/cgroup":
		return (record.fsType == "cgroup" || record.fsType == "cgroup2") && hasOption(record, "ro")
	case "/proc/acpi", "/proc/scsi", "/sys/firmware", "/sys/devices/virtual/powercap":
		return record.fsType == "tmpfs" && hasOption(record, "ro")
	case "/proc/asound", "/proc/interrupts", "/proc/kcore", "/proc/keys", "/proc/latency_stats", "/proc/sched_debug", "/proc/timer_list", "/proc/timer_stats":
		return record.source == "tmpfs" || record.source == "/dev/null"
	default:
		return false
	}
}

func verifyDevices(records []mountRecord) (bool, error) {
	for _, record := range records {
		if suspiciousMount(record) {
			return false, nil
		}
	}
	entries, err := os.ReadDir("/dev")
	if err != nil {
		return false, isolationBlocked()
	}
	for _, entry := range entries {
		switch entry.Name() {
		case "core", "fd", "full", "mqueue", "null", "ptmx", "pts", "random", "shm", "stderr", "stdin", "stdout", "tty", "urandom", "zero":
		default:
			return false, nil
		}
	}
	return true, nil
}

// probeContainerIsolation records only observations made in the helper
// container. It returns no partial proof: missing or malformed proc data is a
// static BLOCKED error rather than an inferred safe value.
func probeContainerIsolation(hostCanaryPort int) (map[string]any, error) {
	statusData, err := os.ReadFile("/proc/self/status")
	if err != nil {
		return nil, isolationBlocked()
	}
	status, err := parseStatus(statusData)
	if err != nil || status.uid != 65534 || os.Getuid() != 65534 || os.Geteuid() != 65534 || !status.capabilitiesZero || !status.noNewPrivileges {
		return nil, isolationBlocked()
	}
	loopbackOnly, err := observeLoopbackOnly()
	if err != nil || !loopbackOnly {
		return nil, isolationBlocked()
	}
	noRoutes, err := observeRoutes()
	if err != nil || !noRoutes {
		return nil, isolationBlocked()
	}
	externalV4, err := boundedDeniedDial("tcp4", "192.0.2.1:80")
	if err != nil {
		return nil, isolationBlocked()
	}
	externalV6, err := boundedDeniedDial("tcp6", "[2001:db8::1]:80")
	if err != nil {
		return nil, isolationBlocked()
	}
	externalDenied := externalV4 && externalV6
	hostDenied, err := boundedHostCanaryDial(hostCanaryPort)
	if err != nil {
		return nil, isolationBlocked()
	}
	mountData, err := os.ReadFile("/proc/self/mountinfo")
	if err != nil {
		return nil, isolationBlocked()
	}
	records, err := parseMountInfo(mountData)
	if err != nil {
		return nil, isolationBlocked()
	}
	mountsVerified := verifyMountRecords(records)
	if !mountsVerified {
		return nil, isolationBlocked()
	}
	devicesVerified, err := verifyDevices(records)
	if err != nil || !devicesVerified {
		return nil, isolationBlocked()
	}
	return map[string]any{
		"uid":                status.uid,
		"capabilities_zero":  status.capabilitiesZero,
		"no_new_privileges":  status.noNewPrivileges,
		"loopback_only":      loopbackOnly,
		"no_routes":          noRoutes,
		"external_denied":    externalDenied,
		"host_canary_denied": hostDenied,
		"mounts_verified":    mountsVerified,
		"devices_verified":   devicesVerified,
	}, nil
}

type boundedBuffer struct {
	data     []byte
	overflow bool
}

func (buffer *boundedBuffer) Write(data []byte) (int, error) {
	if len(data) > maxVersionOutput-len(buffer.data) {
		buffer.overflow = true
		return 0, errors.New("output limit")
	}
	buffer.data = append(buffer.data, data...)
	return len(data), nil
}

func hasPinnedVersion(output []byte) bool {
	for _, field := range strings.Fields(string(output)) {
		field = strings.Trim(field, "()[]{}:,;\"'")
		if field == clientVersion || field == "v"+clientVersion {
			return true
		}
	}
	return false
}

func streamArtifactHash(path string) (string, *os.File, error) {
	info, err := os.Stat(path)
	if err != nil || !info.Mode().IsRegular() || info.Size() < 1 || info.Size() > maxArtifactBytes {
		return "", nil, isolationBlocked()
	}
	file, err := os.Open(path)
	if err != nil {
		return "", nil, isolationBlocked()
	}
	digest, err := verifyPinnedArtifact(file, clientSHA256)
	if err != nil {
		_ = file.Close()
		return "", nil, isolationBlocked()
	}
	return digest, file, nil
}

func verifyPinnedArtifact(reader io.Reader, expected string) (string, error) {
	hasher := sha256.New()
	count, err := io.CopyBuffer(hasher, io.LimitReader(reader, maxArtifactBytes+1), make([]byte, 64<<10))
	if err != nil || count < 1 || count > maxArtifactBytes {
		return "", isolationBlocked()
	}
	digest := hex.EncodeToString(hasher.Sum(nil))
	if digest != expected {
		return "", isolationBlocked()
	}
	return digest, nil
}

func boundedVersion(path string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, path, "--version")
	command.WaitDelay = 250 * time.Millisecond
	command.Env = []string{"PATH=/usr/bin:/bin", "HOME=/home/qharness", "TMPDIR=/tmp", "LANG=C", "NO_COLOR=1"}
	command.Stdin = strings.NewReader("")
	var stdout, stderr boundedBuffer
	command.Stdout = &stdout
	command.Stderr = &stderr
	if err := command.Run(); err != nil || ctx.Err() != nil || stdout.overflow || stderr.overflow {
		return nil, isolationBlocked()
	}
	return stdout.data, nil
}

// verifiedLinuxClient verifies the mounted artifact and its bounded version
// command. It intentionally does not call probeContainerIsolation: the caller
// owns the host-canary port and must run the isolation proof first.
func verifiedLinuxClient() (map[string]any, error) {
	if runtime.GOOS != clientPlatform || runtime.GOARCH != clientArch {
		return nil, isolationBlocked()
	}
	digest, file, err := streamArtifactHash(clientArtifactPath)
	if err != nil {
		return nil, isolationBlocked()
	}
	defer file.Close()
	versionOutput, err := boundedVersion(clientArtifactPath)
	if err != nil || !hasPinnedVersion(versionOutput) {
		return nil, isolationBlocked()
	}
	return map[string]any{
		"name":     filepath.Base(clientArtifactPath),
		"version":  clientVersion,
		"platform": runtime.GOOS,
		"arch":     runtime.GOARCH,
		// The exact official linux/arm64/musl release hash above, rather
		// than the helper's libc or a filename, establishes this value.
		"libc":          clientLibc,
		"binary_sha256": digest,
	}, nil
}
