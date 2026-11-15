package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"reflect"
	"strings"
	"sync"
	"syscall"
	"time"
)

const protocol = "qharness-linux-replay/v1"
const maxConfig = 128 << 10
const maxOutput = 2 << 20

type config struct {
	Protocol       string   `json:"protocol"`
	FixturePath    string   `json:"fixture_path"`
	Client         client   `json:"client"`
	Launch         launch   `json:"launch"`
	Consumer       consumer `json:"consumer"`
	HostCanaryPort int      `json:"host_canary_port"`
	Limits         limits   `json:"limits"`
}
type client struct {
	Name         string `json:"name"`
	Version      string `json:"version"`
	Platform     string `json:"platform"`
	Arch         string `json:"arch"`
	Libc         string `json:"libc"`
	BinarySHA256 string `json:"binary_sha256"`
}
type launch struct {
	Argv     []string          `json:"argv"`
	Env      map[string]string `json:"env"`
	Settings map[string]any    `json:"settings"`
}
type consumer struct {
	AuthMode string `json:"auth_mode"`
}
type limits struct {
	MaxRequests int `json:"max_requests"`
	MaxBytes    int `json:"max_bytes"`
	DeadlineMS  int `json:"deadline_ms"`
}
type frame struct {
	Kind string `json:"kind"`
	Data string `json:"data"`
}
type fixture struct {
	Request struct {
		Method string `json:"method"`
		Path   string `json:"path"`
	} `json:"request"`
	Response struct {
		Frames []frame `json:"frames"`
	} `json:"response"`
	Expected struct {
		Consumer struct {
			Marker string `json:"marker"`
		} `json:"consumer"`
	} `json:"expected"`
}
type counters struct{ Requests, BodyReadCalls, AuthValidated int }
type event struct {
	Type                                   string
	Protocol                               string
	Client                                 any
	Isolation                              any
	Listening                              any
	Method, Path                           string
	Status                                 int
	AuthValid, BodyRead                    bool
	BodyBytes                              int
	ExitCode                               int
	TimedOut, Overflow                     bool
	Signal                                 any
	StdoutB64, StderrB64                   string
	Requests, BodyReadCalls, ProviderCalls int
	AuthValidated                          bool
	Listeners                              any
	ListenerClosure                        bool
	Error                                  string
}

var emitMu sync.Mutex

func (e event) MarshalJSON() ([]byte, error) {
	m := map[string]any{"type": e.Type}
	switch e.Type {
	case "ready":
		m["protocol"], m["client"], m["isolation"], m["listening"] = e.Protocol, e.Client, e.Isolation, e.Listening
	case "request":
		m["method"], m["path"], m["status"] = e.Method, e.Path, e.Status
		m["auth_valid"], m["body_read"], m["body_bytes"] = e.AuthValid, e.BodyRead, e.BodyBytes
	case "consumer":
		m["exit_code"], m["timed_out"], m["overflow"], m["signal"] = e.ExitCode, e.TimedOut, e.Overflow, e.Signal
		m["stdout_b64"], m["stderr_b64"] = e.StdoutB64, e.StderrB64
	case "complete":
		m["requests"], m["body_read_calls"], m["auth_validated"], m["provider_calls"] = e.Requests, e.BodyReadCalls, e.AuthValidated, e.ProviderCalls
		m["listeners"], m["listener_closure"] = e.Listeners, e.ListenerClosure
		if e.Listeners == nil {
			m["listeners"] = []ownedListener{}
		}
	}
	if e.Error != "" {
		m["error"] = e.Error
	}
	return json.Marshal(m)
}
func emit(e event) {
	emitMu.Lock()
	defer emitMu.Unlock()
	_ = json.NewEncoder(os.Stdout).Encode(e)
}

func valid(r *http.Request, mode, want string) bool {
	key := "X-Goog-Api-Key"
	value := want
	if mode == "gateway-bearer" {
		key, value = "Authorization", "Bearer "+want
	} else if mode != "native-api-key" {
		return false
	}
	if (mode == "native-api-key" && want != "qharness-synthetic-local-only") || (mode == "gateway-bearer" && want != "synthetic-gateway-api-key") {
		return false
	}
	count := 0
	for k, values := range r.Header {
		switch strings.ToLower(k) {
		case "authorization", "x-goog-api-key", "x-api-key", "api-key", "goog-api-key":
			if !strings.EqualFold(k, key) || len(values) != 1 || values[0] != value {
				return false
			}
			count++
		}
	}
	return count == 1
}

func validate(c config) error {
	bad := errors.New("config_rejected")
	if c.Protocol != protocol || c.FixturePath != "/opt/qharness/fixture.json" || c.HostCanaryPort < 1 || c.HostCanaryPort > 65535 || c.HostCanaryPort == 18080 {
		return bad
	}
	if c.Client != (client{"agy", "1.5.9", "linux", "arm64", "musl", "d81a2547df4d29e391725219c1908056885abc6c73fd23819553f622ac9b098d"}) {
		return bad
	}
	if c.Limits.MaxRequests < 1 || c.Limits.MaxRequests > 32 || c.Limits.MaxBytes < 1 || c.Limits.MaxBytes > maxOutput || c.Limits.DeadlineMS < 1000 || c.Limits.DeadlineMS > 30000 {
		return bad
	}
	prompt := "Reply with the supplied response text. Do not use tools."
	var argv []string
	var env map[string]string
	var settings map[string]any
	switch c.Consumer.AuthMode {
	case "gateway-bearer":
		argv = []string{"/opt/agy", "--print", prompt, "--output-format", "json"}
		env = map[string]string{"AGY_CLI_HIDE_LOGO": "1", "AGY_CLI_HIDE_ACCOUNT_INFO": "1", "AGY_LLM_GATEWAY_URL": "http://127.0.0.1:18080", "AGY_LLM_GATEWAY_API_KEY": "synthetic-gateway-api-key", "AGY_LLM_GATEWAY_MODELS": "gemini-2.0-flash,gemini-2.5-pro"}
	case "native-api-key":
		argv = []string{"/opt/agy", "--print", prompt, "--model", "gemini-3.1-pro-low", "--settings", `{"modelProvider":"gemini"}`, "--output-format", "stream-json", "--print-timeout", "15s"}
		env = map[string]string{"GEMINI_API_KEY": "qharness-synthetic-local-only", "GOOGLE_GEMINI_BASE_URL": "http://127.0.0.1:18080", "GOOGLE_GENAI_USE_VERTEXAI": "false"}
		settings = map[string]any{"security": map[string]any{"auth": map[string]any{"selectedType": "gemini-api-key"}}, "telemetry": map[string]any{"enabled": false}, "privacy": map[string]any{"usageStatisticsEnabled": false}}
	default:
		return bad
	}
	for k, v := range map[string]string{"HOME": "/home/qharness", "TMPDIR": "/tmp", "PATH": "/usr/bin:/bin", "LANG": "C.UTF-8", "TERM": "dumb", "NO_COLOR": "1"} {
		env[k] = v
	}
	if !reflect.DeepEqual(c.Launch.Argv, argv) || !reflect.DeepEqual(c.Launch.Env, env) || !reflect.DeepEqual(c.Launch.Settings, settings) {
		return bad
	}
	return nil
}

func decodeConfig(reader io.Reader) (config, error) {
	var c config
	raw, err := io.ReadAll(io.LimitReader(reader, maxConfig+1))
	if err != nil || len(raw) > maxConfig {
		return c, errors.New("config_rejected")
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if dec.Decode(&c) != nil || dec.Decode(new(any)) != io.EOF || validate(c) != nil {
		return c, errors.New("config_rejected")
	}
	return c, nil
}
func decodeFixture(reader io.Reader) (fixture, error) {
	var f fixture
	raw, err := io.ReadAll(io.LimitReader(reader, maxOutput+1))
	if err != nil || len(raw) > maxOutput {
		return f, errors.New("fixture_rejected")
	}
	if json.Unmarshal(raw, &f) != nil || validateFixture(f) != nil {
		return f, errors.New("fixture_rejected")
	}
	return f, nil
}
func validateFixture(f fixture) error {
	bad := errors.New("fixture_rejected")
	if f.Request.Method != "POST" || !strings.HasPrefix(f.Request.Path, "/") || strings.HasPrefix(f.Request.Path, "//") || strings.ContainsAny(f.Request.Path, "\r\n#") || len(f.Response.Frames) == 0 {
		return bad
	}
	target, err := url.ParseRequestURI(f.Request.Path)
	if err != nil || target.IsAbs() || target.Host != "" || target.RequestURI() != f.Request.Path {
		return bad
	}
	marker := f.Expected.Consumer.Marker
	if len(marker) < 8 || len(marker) > 100 || strings.Contains("Reply with the supplied response text. Do not use tools.", marker) {
		return bad
	}
	for _, ch := range marker {
		if !(ch >= 'A' && ch <= 'Z' || ch >= '0' && ch <= '9' || ch == '_') {
			return bad
		}
	}
	for _, fr := range f.Response.Frames {
		data := strings.ReplaceAll(fr.Data, "\r\n", "\n")
		if fr.Kind != "sse" || data == "" || !strings.HasSuffix(data, "\n\n") || strings.ContainsRune(data, '\r') {
			return bad
		}
		hasData := false
		for _, line := range strings.Split(data, "\n") {
			if strings.HasPrefix(line, "data:") {
				hasData = true
				continue
			}
			if line == "" || strings.HasPrefix(line, ":") || strings.HasPrefix(line, "event:") || strings.HasPrefix(line, "id:") || strings.HasPrefix(line, "retry:") {
				continue
			}
			return bad
		}
		if !hasData {
			return bad
		}
	}
	return nil
}

func reject(w http.ResponseWriter, status int) {
	if h, ok := w.(http.Hijacker); ok {
		conn, rw, err := h.Hijack()
		if err == nil {
			defer conn.Close()
			_ = conn.SetWriteDeadline(time.Now().Add(time.Second))
			fmt.Fprintf(rw, "HTTP/1.1 %d %s\r\nConnection: close\r\nContent-Length: 0\r\n\r\n", status, http.StatusText(status))
			_ = rw.Flush()
			return
		}
	}
	w.Header().Set("Connection", "close")
	w.WriteHeader(status)
}

func newHTTPHandler(c config, f fixture, observe func() error, send func(event)) (http.Handler, func() counters) {
	if send == nil {
		send = func(event) {}
	}
	var mu sync.Mutex
	var counts counters
	want := c.Launch.Env["GEMINI_API_KEY"]
	if c.Consumer.AuthMode == "gateway-bearer" {
		want = c.Launch.Env["AGY_LLM_GATEWAY_API_KEY"]
	}
	snapshot := func() counters { mu.Lock(); defer mu.Unlock(); return counts }
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		if counts.Requests >= c.Limits.MaxRequests {
			mu.Unlock()
			reject(w, 429)
			return
		}
		counts.Requests++
		mu.Unlock()
		ok := valid(r, c.Consumer.AuthMode, want)
		status := 401
		if ok {
			mu.Lock()
			counts.AuthValidated++
			mu.Unlock()
			status = 404
		}
		if !ok || r.Method != f.Request.Method || r.RequestURI != f.Request.Path {
			send(event{Type: "request", Method: f.Request.Method, Path: f.Request.Path, Status: status, AuthValid: ok})
			reject(w, status)
			return
		}
		if observe != nil && observe() != nil {
			reject(w, 503)
			return
		}
		mu.Lock()
		counts.BodyReadCalls++
		mu.Unlock()
		n, err := io.Copy(io.Discard, io.LimitReader(r.Body, int64(c.Limits.MaxBytes)+1))
		if err != nil || n > int64(c.Limits.MaxBytes) {
			reject(w, 413)
			return
		}
		if observe != nil && observe() != nil {
			reject(w, 503)
			return
		}
		send(event{Type: "request", Method: r.Method, Path: r.RequestURI, Status: 200, AuthValid: true, BodyRead: true, BodyBytes: int(n)})
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(200)
		for _, frame := range f.Response.Frames {
			if _, err := io.WriteString(w, frame.Data); err != nil {
				return
			}
			if fl, ok := w.(http.Flusher); ok {
				fl.Flush()
			}
		}
	})
	return handler, snapshot
}

type outputCapture struct {
	mu       sync.Mutex
	out, err bytes.Buffer
	overflow bool
}
type outputWriter struct {
	capture *outputCapture
	stderr  bool
}

func (w outputWriter) Write(p []byte) (int, error) {
	s := w.capture
	s.mu.Lock()
	defer s.mu.Unlock()
	n := len(p)
	remaining := maxOutput - s.out.Len() - s.err.Len()
	if len(p) > remaining {
		p = p[:remaining]
		s.overflow = true
	}
	if w.stderr {
		_, _ = s.err.Write(p)
	} else {
		_, _ = s.out.Write(p)
	}
	return n, nil
}

func run() {
	fail := func(code string) { emit(event{Type: "complete", Error: code}) }
	c, err := decodeConfig(os.Stdin)
	if err != nil {
		fail("config_rejected")
		return
	}
	isolation, err := probeContainerIsolation(c.HostCanaryPort)
	if err != nil {
		fail("isolation_unverified")
		return
	}
	pinned, err := verifiedLinuxClient()
	if err != nil {
		fail("client_pin_unverified")
		return
	}
	for key, value := range map[string]string{"name": c.Client.Name, "version": c.Client.Version, "platform": c.Client.Platform, "arch": c.Client.Arch, "libc": c.Client.Libc, "binary_sha256": c.Client.BinarySHA256} {
		if pinned[key] != value {
			fail("client_pin_unverified")
			return
		}
	}
	file, err := os.Open(c.FixturePath)
	if err != nil {
		fail("fixture_rejected")
		return
	}
	f, err := decodeFixture(file)
	_ = file.Close()
	if err != nil {
		fail("fixture_rejected")
		return
	}
	if c.Launch.Settings != nil {
		if os.MkdirAll("/home/qharness/.gemini", 0700) != nil {
			fail("settings_failed")
			return
		}
		settings, er := json.Marshal(c.Launch.Settings)
		if er != nil || os.WriteFile("/home/qharness/.gemini/settings.json", settings, 0600) != nil {
			fail("settings_failed")
			return
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(c.Limits.DeadlineMS)*time.Millisecond)
	defer cancel()
	var tracker *listenerTracker
	var trackerMu sync.Mutex
	var observationErr error
	observed := false
	observe := func() error {
		trackerMu.Lock()
		defer trackerMu.Unlock()
		if tracker == nil {
			observationErr = errors.New("listener_tracker_unavailable")
			return observationErr
		}
		if er := tracker.Observe(); er != nil {
			observationErr = er
			cancel()
			return er
		}
		observed = true
		return nil
	}
	handler, snapshot := newHTTPHandler(c, f, observe, emit)
	ln, err := net.Listen("tcp4", "127.0.0.1:18080")
	if err != nil {
		fail("listener_bind_failed")
		return
	}
	srv := &http.Server{Handler: handler, ReadHeaderTimeout: time.Second, ReadTimeout: 5 * time.Second, WriteTimeout: 5 * time.Second, IdleTimeout: time.Second, MaxHeaderBytes: 16384}
	serverDone := make(chan error, 1)
	go func() { serverDone <- srv.Serve(ln) }()
	emit(event{Type: "ready", Protocol: protocol, Client: c.Client, Isolation: isolation, Listening: map[string]any{"address": "127.0.0.1", "port": 18080}})
	cmd := exec.CommandContext(ctx, "/opt/agy", c.Launch.Argv[1:]...)
	cmd.Env = nil
	for k, v := range c.Launch.Env {
		cmd.Env = append(cmd.Env, k+"="+v)
	}
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.WaitDelay = time.Second
	cmd.Cancel = func() error {
		if cmd.Process == nil {
			return os.ErrProcessDone
		}
		return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
	}
	var captured outputCapture
	cmd.Stdout = outputWriter{capture: &captured}
	cmd.Stderr = outputWriter{capture: &captured, stderr: true}
	trackerMu.Lock()
	err = cmd.Start()
	if err == nil {
		tracker, observationErr = newListenerTracker(cmd.Process.Pid)
		trackerMu.Unlock()
		if observationErr != nil {
			cancel()
		}
		err = cmd.Wait()
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
	} else {
		trackerMu.Unlock()
	}
	exitCode := -1
	var signal any
	if err == nil {
		exitCode = 0
	} else {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			exitCode = exitErr.ExitCode()
			if status, ok := exitErr.Sys().(syscall.WaitStatus); ok && status.Signaled() {
				signal = status.Signal().String()
			}
		}
	}
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), time.Second)
	shutdownErr := srv.Shutdown(shutdownCtx)
	shutdownCancel()
	if shutdownErr != nil {
		_ = srv.Close()
	}
	serveErr := <-serverDone
	var evidence []ownedListener
	closureErr := errors.New("listener_tracker_unavailable")
	trackerMu.Lock()
	if tracker != nil {
		evidence, closureErr = tracker.CloseEvidence(time.Second)
	}
	if observationErr != nil {
		closureErr = observationErr
	}
	trackerMu.Unlock()
	captured.mu.Lock()
	emit(event{Type: "consumer", ExitCode: exitCode, Signal: signal, TimedOut: errors.Is(ctx.Err(), context.DeadlineExceeded), Overflow: captured.overflow, StdoutB64: base64.StdEncoding.EncodeToString(captured.out.Bytes()), StderrB64: base64.StdEncoding.EncodeToString(captured.err.Bytes())})
	captured.mu.Unlock()
	counts := snapshot()
	result := event{Type: "complete", Requests: counts.Requests, BodyReadCalls: counts.BodyReadCalls, AuthValidated: counts.Requests > 0 && counts.AuthValidated == counts.Requests, Listeners: evidence, ListenerClosure: closureErr == nil}
	if observationErr != nil {
		if observed {
			result.Error = "listener_late_observation_failed"
		} else {
			result.Error = "listener_initial_observation_failed"
		}
	} else if closureErr != nil {
		result.Error = "listener_evidence_failed"
	} else if shutdownErr != nil || !errors.Is(serveErr, http.ErrServerClosed) {
		result.Error = "server_shutdown_failed"
	}
	emit(result)
}
func main() { run() }
