package main

import (
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
)

type fakeListenerProcFS struct {
	files map[string][]byte
	links map[string]string
	dirs  map[string][]string
}

func (f *fakeListenerProcFS) readFile(path string) ([]byte, error) {
	data, ok := f.files[path]
	if !ok {
		return nil, os.ErrNotExist
	}
	return append([]byte(nil), data...), nil
}

func (f *fakeListenerProcFS) readDir(path string) ([]string, error) {
	entries, ok := f.dirs[path]
	if !ok {
		return nil, os.ErrNotExist
	}
	return append([]string(nil), entries...), nil
}

func (f *fakeListenerProcFS) readLink(path string) (string, error) {
	link, ok := f.links[path]
	if !ok {
		return "", os.ErrNotExist
	}
	return link, nil
}

func listenerTestStat(pid, ppid, pgrp int, start uint64) []byte {
	fields := make([]string, 20)
	fields[0] = "S"
	fields[1] = strconv.Itoa(ppid)
	fields[2] = strconv.Itoa(pgrp)
	for i := 3; i < len(fields); i++ {
		fields[i] = "0"
	}
	fields[19] = strconv.FormatUint(start, 10)
	return []byte(strconv.Itoa(pid) + " (agy) " + strings.Join(fields, " ") + "\n")
}

func listenerTestTCP(inode, address, port string) []byte {
	return []byte("  sl  local_address rem_address   st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode\n" +
		"   0: " + address + ":" + port + " 00000000:0000 0A 00000000:00000000 00:00000000 00000000 0 0 " + inode + " 1 0000000000000000 100 0 10 0\n")
}

func listenerTestFixture() (*fakeListenerProcFS, *listenerTracker) {
	fs := &fakeListenerProcFS{
		files: map[string][]byte{
			"/proc/100/stat":     listenerTestStat(100, 1, 100, 1000),
			"/proc/101/stat":     listenerTestStat(101, 100, 100, 2000),
			"/proc/100/net/tcp":  listenerTestTCP("12345", "0100007F", "A8C1"),
			"/proc/100/net/tcp6": []byte("  sl  local_address rem_address   st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode\n"),
			"/proc/101/net/tcp":  listenerTestTCP("12345", "0100007F", "A8C1"),
			"/proc/101/net/tcp6": []byte("  sl  local_address rem_address   st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode\n"),
			"/proc/net/tcp":      []byte("  sl  local_address rem_address   st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode\n"),
			"/proc/net/tcp6":     []byte("  sl  local_address rem_address   st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode\n"),
		},
		links: map[string]string{
			"/proc/100/ns/net": "net:[1]",
			"/proc/101/ns/net": "net:[1]",
			"/proc/100/fd/0":   "pipe:[9]",
			"/proc/101/fd/3":   "socket:[12345]",
		},
		dirs: map[string][]string{
			"/proc":        {"100", "101", "self"},
			"/proc/100/fd": {"0"},
			"/proc/101/fd": {"3"},
		},
	}
	clock := time.Unix(0, 0)
	now := func() time.Time { return clock }
	tracker, err := newListenerTrackerWithFS(100, "/proc", fs, now)
	if err != nil {
		panic(err)
	}
	tracker.sleep = func(time.Duration) {}
	return fs, tracker
}

func TestListenerTrackerUsesDescendantSocketInodeOwnership(t *testing.T) {
	_, tracker := listenerTestFixture()
	if err := tracker.Observe(); err != nil {
		t.Fatal(err)
	}
	listeners := tracker.observedListeners()
	if len(listeners) != 1 {
		t.Fatalf("got %d listeners, want one", len(listeners))
	}
	got := listeners[0]
	if got.OwnerPID != 101 || got.OwnerProcessGroup != 100 || got.Address != "127.0.0.1" || got.Port != 43201 || got.StartTicks != 2000 {
		t.Fatalf("unexpected listener evidence: %+v", got)
	}
	if got.NetworkNamespace != "net:[1]" || got.FirstSeenMS != 0 || got.LastSeenMS != 0 {
		t.Fatalf("unexpected evidence metadata: %+v", got)
	}
}

func TestListenerTrackerRejectsOwnedNonLoopback(t *testing.T) {
	fs, tracker := listenerTestFixture()
	fs.files["/proc/100/net/tcp"] = listenerTestTCP("12345", "00000000", "A8C1")
	fs.files["/proc/101/net/tcp"] = listenerTestTCP("12345", "00000000", "A8C1")
	if err := tracker.Observe(); err == nil {
		t.Fatal("wildcard listener was accepted")
	}
}

func TestListenerTrackerRejectsPIDReuse(t *testing.T) {
	fs, tracker := listenerTestFixture()
	if err := tracker.Observe(); err != nil {
		t.Fatal(err)
	}
	fs.files["/proc/101/stat"] = listenerTestStat(101, 100, 100, 9999)
	if err := tracker.Observe(); err == nil {
		t.Fatal("reused descendant pid was accepted")
	}
}

func TestListenerTrackerRejectsForeignPIDOwnership(t *testing.T) {
	fs, tracker := listenerTestFixture()
	fs.files["/proc/102/stat"] = listenerTestStat(102, 1, 102, 3000)
	fs.links["/proc/102/ns/net"] = "net:[1]"
	fs.links["/proc/102/fd/4"] = "socket:[12345]"
	fs.dirs["/proc/102/fd"] = []string{"4"}
	fs.dirs["/proc"] = append(fs.dirs["/proc"], "102")
	if err := tracker.Observe(); err == nil {
		t.Fatal("foreign listener owner was accepted")
	}
}
func TestListenerTrackerCloseEvidenceRequiresRootAndDescendantExit(t *testing.T) {
	fs, tracker := listenerTestFixture()
	if err := tracker.Observe(); err != nil {
		t.Fatal(err)
	}
	delete(fs.files, "/proc/100/stat")
	delete(fs.files, "/proc/101/stat")
	delete(fs.files, "/proc/100/net/tcp")
	delete(fs.files, "/proc/100/net/tcp6")
	delete(fs.files, "/proc/101/net/tcp")
	delete(fs.files, "/proc/101/net/tcp6")
	delete(fs.dirs, "/proc/100/fd")
	delete(fs.dirs, "/proc/101/fd")
	delete(fs.links, "/proc/100/ns/net")
	delete(fs.links, "/proc/101/ns/net")
	closed, err := tracker.CloseEvidence(time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if len(closed) != 1 || closed[0].Port != 43201 {
		t.Fatalf("unexpected closure evidence: %+v", closed)
	}
}

func TestListenerTrackerRejectsEmptyObservation(t *testing.T) {
	_, tracker := listenerTestFixture()
	tracker.fs.(*fakeListenerProcFS).files["/proc/100/net/tcp"] = []byte("  sl  local_address rem_address   st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode\n")
	tracker.fs.(*fakeListenerProcFS).files["/proc/101/net/tcp"] = []byte("  sl  local_address rem_address   st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode\n")
	if err := tracker.Observe(); err == nil {
		t.Fatal("empty listener observation was accepted")
	}
}
