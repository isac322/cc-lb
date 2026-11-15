package main

import (
	"net"
	"strings"
	"syscall"
	"testing"
)

func TestParseStatusNormalAndTampered(t *testing.T) {
	normal := []byte("Name:\thelper\nUid:\t65534\t65534\t65534\t65534\nCapEff:\t0000000000000000\nNoNewPrivs:\t1\n")
	status, err := parseStatus(normal)
	if err != nil || status.uid != 65534 || !status.capabilitiesZero || !status.noNewPrivileges {
		t.Fatalf("normal status was not accepted: %#v %v", status, err)
	}
	for _, tampered := range [][]byte{
		[]byte("Uid:\t65534\t65534\t65534\t65534\nCapEff:\t0000000000000001\nNoNewPrivs:\t1\n"),
		[]byte("Uid:\t65534\t65534\t65534\t65534\nCapEff:\t0000000000000000\nNoNewPrivs:\t0\n"),
		[]byte("Uid:\t65534\t65534\t65534\t65534\nNoNewPrivs:\t1\n"),
	} {
		if _, err := parseStatus(tampered); err == nil {
			t.Fatal("tampered status was accepted")
		}
	}
}

func TestParseRoutesNormalAndTampered(t *testing.T) {
	v4, err := parseIPv4Routes([]byte("Iface\tDestination\tGateway\tFlags\tRefCnt\tUse\tMetric\tMask\tMTU\tWindow\tIRTT\n"))
	if err != nil || v4.defaultRoute || v4.nonLoopbackRoute {
		t.Fatalf("empty IPv4 route table was not accepted as empty: %#v %v", v4, err)
	}
	v6, err := parseIPv6Routes(nil)
	if err != nil || v6.defaultRoute || v6.nonLoopbackRoute {
		t.Fatalf("empty IPv6 route table was not accepted as empty: %#v %v", v6, err)
	}
	defaultV4 := []byte("Iface\tDestination\tGateway\tFlags\tRefCnt\tUse\tMetric\tMask\tMTU\tWindow\tIRTT\neth0\t00000000\t00000000\t0001\t0\t0\t0\t00000000\t0\t0\t0\n")
	v4, err = parseIPv4Routes(defaultV4)
	if err != nil || !v4.defaultRoute || !v4.nonLoopbackRoute {
		t.Fatalf("tampered IPv4 route was not observed: %#v %v", v4, err)
	}
	if _, err := parseIPv4Routes([]byte("Iface\nmalformed\n")); err == nil {
		t.Fatal("malformed IPv4 route was accepted")
	}
	rejectedDefault := "00000000000000000000000000000000 00 00000000000000000000000000000000 00 00000000000000000000000000000000 ffffffff 00000001 00000000 00200200 lo\n"
	v6, err = parseIPv6Routes([]byte(rejectedDefault))
	if err != nil || v6.defaultRoute || v6.nonLoopbackRoute {
		t.Fatalf("kernel unreachable loopback sentinel was mistaken for a usable route: %#v %v", v6, err)
	}
	usableDefault := strings.Replace(rejectedDefault, "00200200 lo", "00000001 eth0", 1)
	v6, err = parseIPv6Routes([]byte(usableDefault))
	if err != nil || !v6.defaultRoute || !v6.nonLoopbackRoute {
		t.Fatalf("usable IPv6 default route was not detected: %#v %v", v6, err)
	}
	for _, malformed := range []string{"malformed", strings.Replace(rejectedDefault, " 00 ", " zz ", 1)} {
		if _, err := parseIPv6Routes([]byte(malformed)); err == nil {
			t.Fatal("malformed IPv6 route was accepted")
		}
	}
}

func TestParseAndVerifyMountsNormalAndTampered(t *testing.T) {
	fixture := []byte("1 0 0:1 / / ro,relatime - overlay overlay rw\n" +
		"2 1 0:2 / /opt/agy ro,relatime - ext4 /host/agy rw\n" +
		"3 1 0:2 / /opt/qharness/helper ro,relatime - ext4 /host/helper rw\n" +
		"4 1 0:2 / /opt/qharness/fixture.json ro,relatime - ext4 /host/fixture.json rw\n" +
		"5 1 0:5 / /home/qharness rw,nosuid,nodev,noexec,relatime - tmpfs tmpfs rw,size=65536k,mode=700,uid=65534,gid=65534\n" +
		"6 1 0:5 / /tmp rw,nosuid,nodev,noexec,relatime - tmpfs tmpfs rw,size=65536k,mode=700,uid=65534,gid=65534\n" +
		"7 1 0:5 / /var/lib/postgresql rw,nosuid,nodev,noexec,relatime - tmpfs tmpfs rw,size=65536k,mode=700,uid=65534,gid=65534\n")
	records, err := parseMountInfo(fixture)
	if err != nil || !verifyMountRecords(records) {
		t.Fatalf("normal mounts were not accepted: %v", err)
	}
	for _, mutation := range [][2]string{
		{"/ / ro,", "/ / rw,"},
		{"/opt/agy ro,", "/opt/agy rw,"},
		{"nodev,noexec", "nodev"},
		{"mode=700", "mode=755"},
		{"uid=65534", "uid=0"},
		{"size=65536k", "size=131072k"},
		{"relatime - tmpfs", "relatime shared:1 - tmpfs"},
		{"/opt/qharness/helper", "/opt/qharness/not-helper"},
	} {
		mutated := strings.Replace(string(fixture), mutation[0], mutation[1], 1)
		if mutated == string(fixture) {
			t.Fatal("mount mutation did not alter its control fixture")
		}
		parsed, parseErr := parseMountInfo([]byte(mutated))
		if parseErr == nil && verifyMountRecords(parsed) {
			t.Fatalf("unsafe mount mutation accepted: %q", mutation[0])
		}
	}
	for _, extra := range []string{
		"8 1 0:8 / /root/.aws ro - ext4 /host/aws rw\n",
		"8 1 0:8 / /var/run/docker.sock ro - ext4 /host/socket rw\n",
		"8 1 0:8 / /run/dbus ro - ext4 /host/dbus rw\n",
		"8 1 0:8 / /dev/snd rw - tmpfs tmpfs rw\n",
	} {
		parsed, parseErr := parseMountInfo([]byte(string(fixture) + extra))
		if parseErr == nil && verifyMountRecords(parsed) {
			t.Fatal("unexpected sensitive mount accepted")
		}
	}
	standard := string(fixture) +
		"8 1 0:8 / /etc/hosts ro - ext4 /host/hosts rw\n" +
		"9 1 0:9 / /proc rw,nosuid,nodev,noexec - proc proc rw\n" +
		"10 1 0:10 / /dev rw,nosuid - tmpfs tmpfs rw\n" +
		"11 1 0:11 / /dev/pts rw,nosuid,noexec - devpts devpts rw\n" +
		"12 1 0:12 / /dev/shm rw,nosuid,nodev,noexec - tmpfs shm rw\n" +
		"13 1 0:13 / /dev/mqueue rw,nosuid,nodev,noexec - mqueue mqueue rw\n" +
		"14 1 0:14 / /sys ro,nosuid,nodev,noexec - sysfs sysfs ro\n" +
		"15 1 0:15 / /sys/fs/cgroup ro,nosuid,nodev,noexec - cgroup2 cgroup ro\n" +
		"16 1 0:16 / /proc/acpi ro - tmpfs tmpfs ro\n" +
		"17 1 0:17 /null /proc/interrupts rw - tmpfs tmpfs rw\n"
	parsed, parseErr := parseMountInfo([]byte(standard))
	if parseErr != nil || !verifyMountRecords(parsed) {
		t.Fatalf("standard Docker pseudo mounts were rejected: %v", parseErr)
	}
	for _, tampered := range [][]byte{
		[]byte("1 0 0:1 / / rw,relatime - overlay overlay rw\n"),
		[]byte("1 0 0:1 / / ro,relatime - overlay overlay rw\n2 1 0:2 / /opt/agy rw,relatime - ext4 /host/agy rw\n"),
		[]byte("1 0 0:1 / / ro,relatime - overlay overlay rw\n2 1 0:2 / /opt/agy ro,relatime - ext4 /host/agy rw\n3 1 0:2 / /opt/qharness/helper ro,relatime - ext4 /host/helper rw\n4 1 0:2 / /opt/qharness/fixture.json ro,relatime - ext4 /host/fixture.json rw\n5 1 0:5 / /home/qharness rw,nosuid,nodev,noexec,relatime - tmpfs tmpfs rw,size=65536k,mode=700,uid=65534,gid=65534\n6 1 0:5 / /tmp rw,nosuid,nodev,noexec,relatime - tmpfs tmpfs rw,size=65536k,mode=700,uid=65534,gid=65534\n7 1 0:5 / /var/lib/postgresql rw,nosuid,nodev,noexec,relatime shared:1 - tmpfs tmpfs rw,size=65536k,mode=700,uid=65534,gid=65534\n"),
	} {
		records, err = parseMountInfo(tampered)
		if err == nil && verifyMountRecords(records) {
			t.Fatal("tampered mounts were accepted")
		}
	}
}

func TestSocketResultClassification(t *testing.T) {
	for _, denied := range []error{
		&net.OpError{Err: syscall.ENETUNREACH},
		&net.OpError{Err: syscall.EHOSTUNREACH},
		&net.OpError{Err: syscall.EPERM},
		&net.OpError{Err: syscall.EACCES},
	} {
		if !isIsolationDeniedError(denied) {
			t.Fatalf("denied socket result was not classified: %v", denied)
		}
	}
	if isIsolationDeniedError(&net.OpError{Err: syscall.ETIMEDOUT}) {
		t.Fatal("timeout was accepted as network isolation evidence")
	}
	if !isConnectionRefusedError(&net.OpError{Err: syscall.ECONNREFUSED}) {
		t.Fatal("connection refusal was not classified")
	}
	if isConnectionRefusedError(&net.OpError{Err: syscall.ETIMEDOUT}) {
		t.Fatal("timeout was classified as connection refusal")
	}
}

func TestPinnedVersionMismatch(t *testing.T) {
	if hasPinnedVersion([]byte("agy 1.5.8\n")) {
		t.Fatal("mismatched version was accepted")
	}
	if !hasPinnedVersion([]byte("agy 1.5.9\n")) {
		t.Fatal("pinned version was rejected")
	}
	if hasPinnedVersion([]byte("agy 1.5.90\n")) {
		t.Fatal("version prefix was accepted")
	}
}

func TestPinnedArtifactMismatch(t *testing.T) {
	const knownSHA256 = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
	digest, err := verifyPinnedArtifact(strings.NewReader("abc"), knownSHA256)
	if err != nil || digest != knownSHA256 {
		t.Fatalf("matching artifact was not accepted: %v", err)
	}
	for _, artifact := range []string{"", "abd", "abc\n"} {
		if _, err := verifyPinnedArtifact(strings.NewReader(artifact), knownSHA256); err == nil || err.Error() != isolationBlockedCode {
			t.Fatal("mismatched artifact was not blocked with a static code")
		}
	}
}

func TestCanaryPortRejectsHelperAndInvalidPorts(t *testing.T) {
	for _, port := range []int{-1, 0, helperPort, 65536} {
		if allowed, err := boundedHostCanaryDial(port); err == nil || allowed {
			t.Fatal("invalid canary target accepted")
		}
	}
}

func TestVersionOutputBound(t *testing.T) {
	var output boundedBuffer
	if _, err := output.Write(make([]byte, maxVersionOutput)); err != nil || output.overflow {
		t.Fatal("bounded control output rejected")
	}
	if _, err := output.Write([]byte("x")); err == nil || !output.overflow {
		t.Fatal("version output overflow accepted")
	}
}

func TestMountPathEscapesFailClosed(t *testing.T) {
	value, err := decodeMountInfoPath("/fixture\\040with\\040spaces")
	if err != nil || value != "/fixture with spaces" {
		t.Fatal("valid mount path escape rejected")
	}
	for _, invalid := range []string{"/fixture\\", "/fixture\\777", "/fixture\\xyz"} {
		if _, err := decodeMountInfoPath(invalid); err == nil {
			t.Fatal("invalid mount path escape accepted")
		}
	}
}