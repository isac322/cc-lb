package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

func testConfig(mode string) config {
	c := config{
		Protocol:    protocol,
		FixturePath: "/opt/qharness/fixture.json",
		Client: client{
			Name: "agy", Version: "1.5.9", Platform: "linux", Arch: "arm64", Libc: "musl",
			BinarySHA256: "d81a2547df4d29e391725219c1908056885abc6c73fd23819553f622ac9b098d",
		},
		Consumer:       consumer{AuthMode: mode},
		HostCanaryPort: 32123,
		Limits:         limits{MaxRequests: 32, MaxBytes: 2 << 20, DeadlineMS: 30000},
		Launch: launch{Env: map[string]string{
			"HOME": "/home/qharness", "TMPDIR": "/tmp", "PATH": "/usr/bin:/bin",
			"LANG": "C.UTF-8", "TERM": "dumb", "NO_COLOR": "1",
		}},
	}
	prompt := "Reply with the supplied response text. Do not use tools."
	if mode == "gateway-bearer" {
		c.Launch.Argv = []string{"/opt/agy", "--print", prompt, "--output-format", "json"}
		c.Launch.Env["AGY_CLI_HIDE_LOGO"] = "1"
		c.Launch.Env["AGY_CLI_HIDE_ACCOUNT_INFO"] = "1"
		c.Launch.Env["AGY_LLM_GATEWAY_URL"] = "http://127.0.0.1:18080"
		c.Launch.Env["AGY_LLM_GATEWAY_API_KEY"] = "synthetic-gateway-api-key"
		c.Launch.Env["AGY_LLM_GATEWAY_MODELS"] = "gemini-2.0-flash,gemini-2.5-pro"
	} else {
		c.Launch.Argv = []string{"/opt/agy", "--print", prompt, "--model", "gemini-3.1-pro-low", "--settings", `{"modelProvider":"gemini"}`, "--output-format", "stream-json", "--print-timeout", "15s"}
		c.Launch.Env["GEMINI_API_KEY"] = "qharness-synthetic-local-only"
		c.Launch.Env["GOOGLE_GEMINI_BASE_URL"] = "http://127.0.0.1:18080"
		c.Launch.Env["GOOGLE_GENAI_USE_VERTEXAI"] = "false"
		c.Launch.Settings = map[string]any{
			"security":  map[string]any{"auth": map[string]any{"selectedType": "gemini-api-key"}},
			"telemetry": map[string]any{"enabled": false},
			"privacy":   map[string]any{"usageStatisticsEnabled": false},
		}
	}
	return c
}

func testFixture() fixture {
	var f fixture
	f.Request.Method = http.MethodPost
	f.Request.Path = "/v1beta/models/gemini:streamGenerateContent?alt=sse&opaque=%2F"
	f.Expected.Consumer.Marker = "QHARNESS_TEST_MARKER"
	f.Response.Frames = []frame{
		{Kind: "sse", Data: ": keepalive\r\n\r\nevent: message\r\ndata: {\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"QHARNESS_TEST_MARKER\"},{\"futurePart\":{\"opaque\":true}}]}}],\"futureEnvelope\":[2,1]}\r\n\r\n"},
		{Kind: "sse", Data: "id: second\ndata: {\"candidates\":[{\"finishReason\":\"STOP\"}]}\n\n"},
	}
	return f
}

func authValue(mode string) string {
	if mode == "gateway-bearer" {
		return "synthetic-gateway-api-key"
	}
	return "qharness-synthetic-local-only"
}

func setTestAuth(r *http.Request, mode string) {
	if mode == "gateway-bearer" {
		r.Header.Set("Authorization", "Bearer "+authValue(mode))
	} else {
		r.Header.Set("X-Goog-Api-Key", authValue(mode))
	}
}

type testBodySpy struct {
	reader io.Reader
	calls  int
	bytes  int
}

func (s *testBodySpy) Read(p []byte) (int, error) {
	s.calls++
	n, err := s.reader.Read(p)
	s.bytes += n
	return n, err
}

func (s *testBodySpy) Close() error { return nil }

type testObservedSSEWriter struct {
	recorder     *httptest.ResponseRecorder
	beforeOutput func()
	started      bool
}

func (w *testObservedSSEWriter) Header() http.Header { return w.recorder.Header() }

func (w *testObservedSSEWriter) WriteHeader(status int) {
	if status == http.StatusOK {
		w.beforeOutput()
		w.started = true
	}
	w.recorder.WriteHeader(status)
}

func (w *testObservedSSEWriter) Write(p []byte) (int, error) {
	w.beforeOutput()
	w.started = true
	return w.recorder.Write(p)
}

func (w *testObservedSSEWriter) Flush() {
	w.beforeOutput()
	w.started = true
	w.recorder.Flush()
}

func requestWithSpy(f fixture, mode, body string) (*http.Request, *testBodySpy) {
	r := httptest.NewRequest(f.Request.Method, f.Request.Path, nil)
	s := &testBodySpy{reader: strings.NewReader(body)}
	r.Body = s
	r.ContentLength = int64(len(body))
	setTestAuth(r, mode)
	return r, s
}

func TestValidateAllowsOnlyPinnedSyntheticLaunch(t *testing.T) {
	for _, mode := range []string{"native-api-key", "gateway-bearer"} {
		t.Run(mode, func(t *testing.T) {
			if err := validate(testConfig(mode)); err != nil {
				t.Fatalf("canonical synthetic config rejected: %v", err)
			}
			cases := []struct {
				name   string
				mutate func(*config)
			}{
				{"protocol", func(c *config) { c.Protocol += "-unknown" }},
				{"relative-fixture", func(c *config) { c.FixturePath = "fixture.json" }},
				{"other-absolute-fixture", func(c *config) { c.FixturePath = "/tmp/fixture.json" }},
				{"fixture-traversal", func(c *config) { c.FixturePath = "/opt/qharness/../qharness/fixture.json" }},
				{"fixture-empty", func(c *config) { c.FixturePath = "" }},
				{"argv-empty", func(c *config) { c.Launch.Argv = nil }},
				{"other-program", func(c *config) { c.Launch.Argv[0] = "/tmp/agy" }},
				{"shell-wrapper", func(c *config) { c.Launch.Argv = append([]string{"/bin/sh", "-c"}, c.Launch.Argv...) }},
				{"program-only", func(c *config) { c.Launch.Argv = []string{"/opt/agy"} }},
				{"extra-argument", func(c *config) { c.Launch.Argv = append(c.Launch.Argv, "--unsafe") }},
				{"empty-environment", func(c *config) { c.Launch.Env = nil }},
				{"extra-environment", func(c *config) { c.Launch.Env["LD_PRELOAD"] = "/tmp/not-a-library" }},
				{"wrong-home", func(c *config) { c.Launch.Env["HOME"] = "/root" }},
				{"wrong-tmpdir", func(c *config) { c.Launch.Env["TMPDIR"] = "/var/tmp" }},
				{"wrong-path", func(c *config) { c.Launch.Env["PATH"] = "/tmp:/usr/bin:/bin" }},
				{"real-looking-key", func(c *config) {
					key := "GEMINI_API_KEY"
					if mode == "gateway-bearer" {
						key = "AGY_LLM_GATEWAY_API_KEY"
					}
					c.Launch.Env[key] = "not-the-known-synthetic-key"
				}},
				{"missing-key", func(c *config) {
					delete(c.Launch.Env, "GEMINI_API_KEY")
					delete(c.Launch.Env, "AGY_LLM_GATEWAY_API_KEY")
				}},
				{"remote-origin", func(c *config) {
					key := "GOOGLE_GEMINI_BASE_URL"
					if mode == "gateway-bearer" {
						key = "AGY_LLM_GATEWAY_URL"
					}
					c.Launch.Env[key] = "https://example.invalid"
				}},
				{"other-loopback-port", func(c *config) {
					key := "GOOGLE_GEMINI_BASE_URL"
					if mode == "gateway-bearer" {
						key = "AGY_LLM_GATEWAY_URL"
					}
					c.Launch.Env[key] = "http://127.0.0.1:18081"
				}},
				{"unknown-auth-mode", func(c *config) { c.Consumer.AuthMode = "automatic" }},
				{"client-name", func(c *config) { c.Client.Name = "other" }},
				{"client-version", func(c *config) { c.Client.Version = "1.5.8" }},
				{"client-platform", func(c *config) { c.Client.Platform = "darwin" }},
				{"client-arch", func(c *config) { c.Client.Arch = "amd64" }},
				{"client-libc", func(c *config) { c.Client.Libc = "glibc" }},
				{"client-hash", func(c *config) { c.Client.BinarySHA256 = strings.Repeat("0", 64) }},
				{"canary-zero", func(c *config) { c.HostCanaryPort = 0 }},
				{"canary-negative", func(c *config) { c.HostCanaryPort = -1 }},
				{"canary-too-large", func(c *config) { c.HostCanaryPort = 65536 }},
				{"canary-helper-port", func(c *config) { c.HostCanaryPort = 18080 }},
				{"request-limit-zero", func(c *config) { c.Limits.MaxRequests = 0 }},
				{"request-limit-negative", func(c *config) { c.Limits.MaxRequests = -1 }},
				{"request-limit-over-cap", func(c *config) { c.Limits.MaxRequests = 33 }},
				{"byte-limit-zero", func(c *config) { c.Limits.MaxBytes = 0 }},
				{"byte-limit-negative", func(c *config) { c.Limits.MaxBytes = -1 }},
				{"byte-limit-over-cap", func(c *config) { c.Limits.MaxBytes = (2 << 20) + 1 }},
				{"deadline-zero", func(c *config) { c.Limits.DeadlineMS = 0 }},
				{"deadline-negative", func(c *config) { c.Limits.DeadlineMS = -1 }},
				{"deadline-over-cap", func(c *config) { c.Limits.DeadlineMS = 30001 }},
			}
			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					c := testConfig(mode)
					tc.mutate(&c)
					if err := validate(c); err == nil {
						t.Fatal("unsafe synthetic config accepted")
					}
				})
			}
		})
	}
}

func TestAuthNegativeNeverReadsBody(t *testing.T) {
	for _, mode := range []string{"native-api-key", "gateway-bearer"} {
		t.Run(mode, func(t *testing.T) {
			header := "X-Goog-Api-Key"
			value := authValue(mode)
			other := "Authorization"
			if mode == "gateway-bearer" {
				header, other = "Authorization", "X-Goog-Api-Key"
				value = "Bearer " + value
			}
			cases := []struct {
				name   string
				change func(http.Header)
			}{
				{"missing", func(h http.Header) { h.Del(header) }},
				{"wrong", func(h http.Header) { h.Set(header, "wrong-synthetic-value") }},
				{"empty", func(h http.Header) { h.Set(header, "") }},
				{"leading-space", func(h http.Header) { h.Set(header, " "+value) }},
				{"trailing-space", func(h http.Header) { h.Set(header, value+" ") }},
				{"comma-combined", func(h http.Header) { h.Set(header, value+","+value) }},
				{"duplicate-identical", func(h http.Header) { h.Add(header, value) }},
				{"duplicate-wrong", func(h http.Header) { h.Add(header, "wrong") }},
				{"duplicate-empty", func(h http.Header) { h.Add(header, "") }},
				{"mixed", func(h http.Header) { h.Set(other, authValue(mode)) }},
				{"mixed-empty", func(h http.Header) { h.Set(other, "") }},
				{"alias-x-api-key", func(h http.Header) { h.Del(header); h.Set("X-Api-Key", authValue(mode)) }},
				{"alias-api-key", func(h http.Header) { h.Del(header); h.Set("Api-Key", authValue(mode)) }},
				{"alias-goog-api-key", func(h http.Header) { h.Del(header); h.Set("Goog-Api-Key", authValue(mode)) }},
				{"canonical-plus-x-api-key", func(h http.Header) { h.Set("X-Api-Key", authValue(mode)) }},
				{"canonical-plus-api-key", func(h http.Header) { h.Set("Api-Key", authValue(mode)) }},
				{"canonical-plus-goog-api-key", func(h http.Header) { h.Set("Goog-Api-Key", authValue(mode)) }},
			}
			if mode == "gateway-bearer" {
				cases = append(cases, struct {
					name   string
					change func(http.Header)
				}{"lowercase-bearer", func(h http.Header) { h.Set(header, "bearer "+authValue(mode)) }})
			}
			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					f := testFixture()
					r, body := requestWithSpy(f, mode, "body-must-not-be-consumed")
					tc.change(r.Header)
					if valid(r, mode, authValue(mode)) {
						t.Fatal("invalid or ambiguous authentication accepted")
					}
					observations := 0
					h, snapshot := newHTTPHandler(testConfig(mode), f, func() error { observations++; return nil }, nil)
					w := httptest.NewRecorder()
					h.ServeHTTP(w, r)
					if w.Code != http.StatusUnauthorized && w.Code != http.StatusForbidden {
						t.Fatalf("auth rejection status = %d", w.Code)
					}
					if body.calls != 0 || body.bytes != 0 || snapshot().BodyReadCalls != 0 || snapshot().AuthValidated != 0 {
						t.Fatalf("rejected auth consumed body or counted as valid: reads=%d bytes=%d counters=%+v", body.calls, body.bytes, snapshot())
					}
					if observations != 0 {
						t.Fatal("rejected auth reached consumer listener observation")
					}
					if strings.Contains(w.Body.String(), "QHARNESS_TEST_MARKER") {
						t.Fatal("rejected auth received replay stream")
					}
				})
			}
		})
	}
	r := httptest.NewRequest(http.MethodPost, "/", nil)
	r.Header.Set("X-Goog-Api-Key", authValue("native-api-key"))
	if valid(r, "unknown-mode", authValue("native-api-key")) || valid(r, "native-api-key", "") {
		t.Fatal("unknown mode or empty expected key accepted")
	}
}

func TestHTTPPreservesSSEAndExactRequestURI(t *testing.T) {
	for _, mode := range []string{"native-api-key", "gateway-bearer"} {
		t.Run(mode, func(t *testing.T) {
			f := testFixture()
			bodyText := `{"contents":[{"parts":[{"text":"synthetic request"}]}]}`
			r, body := requestWithSpy(f, mode, bodyText)
			if !valid(r, mode, authValue(mode)) {
				t.Fatal("canonical authentication rejected")
			}
			beforeBodyObserved := false
			afterBodyObserved := false
			w := httptest.NewRecorder()
			output := &testObservedSSEWriter{recorder: w}
			output.beforeOutput = func() {
				if !beforeBodyObserved || !afterBodyObserved || body.bytes != len(bodyText) {
					t.Errorf("SSE output preceded authenticated body and ownership proof: initial=%t post-body=%t bytes=%d", beforeBodyObserved, afterBodyObserved, body.bytes)
				}
			}
			var events []event
			h, snapshot := newHTTPHandler(testConfig(mode), f, func() error {
				if output.started || w.Body.Len() != 0 || w.Flushed {
					t.Error("ownership observation followed consumer-visible SSE output")
				}
				if body.calls == 0 {
					beforeBodyObserved = true
				} else {
					if !beforeBodyObserved || body.bytes != len(bodyText) {
						t.Errorf("ownership proof order invalid: initial=%t body=%+v", beforeBodyObserved, body)
					}
					afterBodyObserved = true
				}
				return nil
			}, func(e event) { events = append(events, e) })
			h.ServeHTTP(output, r)
			if w.Code != http.StatusOK || !strings.HasPrefix(w.Header().Get("Content-Type"), "text/event-stream") {
				t.Fatalf("not an SSE response: status=%d headers=%v", w.Code, w.Header())
			}
			want := f.Response.Frames[0].Data + f.Response.Frames[1].Data
			if got := w.Body.String(); got != want {
				t.Fatalf("SSE bytes/order/unknown parts changed:\ngot %q\nwant %q", got, want)
			}
			if !w.Flushed {
				t.Fatal("SSE response was not flushed")
			}
			if !beforeBodyObserved || !afterBodyObserved || body.bytes != len(bodyText) || body.calls == 0 {
				t.Fatalf("missing ordered request ownership proof: initial=%t post-body=%t body=%+v", beforeBodyObserved, afterBodyObserved, body)
			}
			if got := snapshot(); got.Requests != 1 || got.BodyReadCalls != 1 || got.AuthValidated != 1 {
				t.Fatalf("incorrect successful request counters: %+v", got)
			}
			if len(events) != 1 {
				t.Fatalf("request events = %d, want one", len(events))
			}
			record := eventObject(t, events[0])
			if record["type"] != "request" || record["method"] != f.Request.Method || record["path"] != f.Request.Path ||
				record["status"] != float64(200) || record["auth_valid"] != true || record["body_read"] != true || record["body_bytes"] != float64(len(bodyText)) {
				t.Fatalf("request evidence lost actual URI/body: %#v", record)
			}
			for key := range record {
				switch key {
				case "type", "method", "path", "status", "auth_valid", "body_read", "body_bytes":
				default:
					t.Errorf("request event has field rejected by host parser: %q", key)
				}
			}
		})
	}
}

func TestHTTPRejectsMethodOrRequestURIMismatchBeforeBody(t *testing.T) {
	f := testFixture()
	for _, tc := range []struct{ name, method, uri string }{
		{"method", http.MethodGet, f.Request.Path},
		{"missing-query", http.MethodPost, strings.Split(f.Request.Path, "?")[0]},
		{"decoded-escape", http.MethodPost, strings.ReplaceAll(f.Request.Path, "%2F", "/")},
		{"different-query-order", http.MethodPost, "/v1beta/models/gemini:streamGenerateContent?opaque=%2F&alt=sse"},
		{"extra-query", http.MethodPost, f.Request.Path + "&unexpected=1"},
		{"different-path", http.MethodPost, "/other?alt=sse&opaque=%2F"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, body := requestWithSpy(f, "native-api-key", "unconsumed")
			r.Method = tc.method
			r.RequestURI = tc.uri
			h, snapshot := newHTTPHandler(testConfig("native-api-key"), f, nil, nil)
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			if w.Code < 400 || w.Code >= 500 || body.calls != 0 || snapshot().BodyReadCalls != 0 {
				t.Fatalf("mismatched request was not rejected before body: status=%d reads=%d counters=%+v", w.Code, body.calls, snapshot())
			}
			if strings.Contains(w.Body.String(), "QHARNESS_TEST_MARKER") {
				t.Fatal("mismatched URI received replay")
			}
		})
	}
}

func TestHTTPBodyLimitBoundary(t *testing.T) {
	for _, size := range []int{0, 3, 4, 5, 64} {
		for _, declared := range []bool{false, true} {
			t.Run(fmt.Sprintf("bytes-%d/content-length-%t", size, declared), func(t *testing.T) {
				c := testConfig("native-api-key")
				c.Limits.MaxBytes = 4
				f := testFixture()
				r, body := requestWithSpy(f, c.Consumer.AuthMode, strings.Repeat("x", size))
				if !declared {
					r.ContentLength = -1
				}
				h, snapshot := newHTTPHandler(c, f, nil, nil)
				w := httptest.NewRecorder()
				h.ServeHTTP(w, r)
				want := http.StatusOK
				if size > c.Limits.MaxBytes {
					want = http.StatusRequestEntityTooLarge
				}
				if w.Code != want {
					t.Fatalf("status=%d want=%d", w.Code, want)
				}
				if body.bytes > c.Limits.MaxBytes+1 {
					t.Fatalf("body read was not bounded: %d", body.bytes)
				}
				if size > c.Limits.MaxBytes && strings.Contains(w.Body.String(), "QHARNESS_TEST_MARKER") {
					t.Fatal("oversized body received replay")
				}
				if size <= c.Limits.MaxBytes && snapshot().BodyReadCalls != 1 {
					t.Fatalf("body evidence missing at boundary: %+v", snapshot())
				}
			})
		}
	}
}

func TestHTTPRequestLimitAndSnapshot(t *testing.T) {
	c := testConfig("native-api-key")
	c.Limits.MaxRequests = 1
	f := testFixture()
	h, snapshot := newHTTPHandler(c, f, nil, nil)
	first, _ := requestWithSpy(f, c.Consumer.AuthMode, "first")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, first)
	if w.Code != http.StatusOK {
		t.Fatalf("first request status=%d", w.Code)
	}
	before := snapshot()
	second, body := requestWithSpy(f, c.Consumer.AuthMode, "must-not-read")
	w = httptest.NewRecorder()
	h.ServeHTTP(w, second)
	if w.Code != http.StatusTooManyRequests || body.calls != 0 {
		t.Fatalf("request budget did not fail before body: status=%d reads=%d", w.Code, body.calls)
	}
	if snapshot().BodyReadCalls != before.BodyReadCalls {
		t.Fatal("over-budget request incremented body-read evidence")
	}
	before.Requests = -100
	if snapshot().Requests < 0 {
		t.Fatal("snapshot mutated handler counters")
	}
}

func TestHTTPObservationFailureIsClosedBeforeBody(t *testing.T) {
	const privateDiagnostic = "untrusted-observer-diagnostic"
	f := testFixture()
	r, body := requestWithSpy(f, "native-api-key", "must-not-read")
	calls := 0
	h, snapshot := newHTTPHandler(testConfig("native-api-key"), f, func() error {
		calls++
		return errors.New(privateDiagnostic)
	}, nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code < 500 || w.Code > 599 || calls != 1 || body.calls != 0 || snapshot().BodyReadCalls != 0 {
		t.Fatalf("proof failure did not close before body: status=%d observations=%d reads=%d counters=%+v", w.Code, calls, body.calls, snapshot())
	}
	if strings.Contains(w.Body.String(), privateDiagnostic) || strings.Contains(w.Body.String(), "QHARNESS_TEST_MARKER") {
		t.Fatal("proof failure exposed raw error or replay")
	}
}

func TestHTTPPostBodyObservationFailureWithholdsSSE(t *testing.T) {
	for _, mode := range []string{"native-api-key", "gateway-bearer"} {
		t.Run(mode, func(t *testing.T) {
			const privateDiagnostic = "untrusted-post-body-observer-diagnostic"
			const bodyText = "synthetic-request-body"
			f := testFixture()
			r, body := requestWithSpy(f, mode, bodyText)
			beforeBodyObserved := false
			postBodyFailed := false
			w := httptest.NewRecorder()
			var events []event
			h, snapshot := newHTTPHandler(testConfig(mode), f, func() error {
				if w.Body.Len() != 0 || w.Flushed {
					t.Error("consumer received stream before post-body ownership proof")
				}
				if body.calls == 0 {
					beforeBodyObserved = true
					return nil
				}
				if !beforeBodyObserved || body.bytes != len(bodyText) {
					t.Errorf("post-body proof did not follow full authenticated body: initial=%t body=%+v", beforeBodyObserved, body)
				}
				postBodyFailed = true
				return errors.New(privateDiagnostic)
			}, func(e event) { events = append(events, e) })
			h.ServeHTTP(w, r)
			if !beforeBodyObserved || !postBodyFailed || body.bytes != len(bodyText) || body.calls == 0 {
				t.Fatalf("post-body failure scenario not exercised: initial=%t failed=%t body=%+v", beforeBodyObserved, postBodyFailed, body)
			}
			if w.Code != http.StatusServiceUnavailable || w.Body.Len() != 0 || w.Flushed || strings.HasPrefix(w.Header().Get("Content-Type"), "text/event-stream") {
				t.Fatalf("failed ownership proof exposed SSE: status=%d bytes=%q flushed=%t headers=%v", w.Code, w.Body.Bytes(), w.Flushed, w.Header())
			}
			if got := snapshot(); got.Requests != 1 || got.AuthValidated != 1 || got.BodyReadCalls != 1 {
				t.Fatalf("post-body failure lost actual request evidence: %+v", got)
			}
			for _, e := range events {
				record := eventObject(t, e)
				if record["status"] == float64(http.StatusOK) || strings.Contains(fmt.Sprint(record), privateDiagnostic) {
					t.Fatalf("failed proof emitted success or raw diagnostic: %#v", record)
				}
			}
		})
	}
}

func TestHTTPCountersConcurrent(t *testing.T) {
	c := testConfig("gateway-bearer")
	f := testFixture()
	var emitted int32
	h, snapshot := newHTTPHandler(c, f, nil, func(event) { atomic.AddInt32(&emitted, 1) })
	const count = 16
	start := make(chan struct{})
	var wg sync.WaitGroup
	for range count {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			r, _ := requestWithSpy(f, c.Consumer.AuthMode, "x")
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			if w.Code != http.StatusOK {
				t.Errorf("concurrent response status=%d", w.Code)
			}
			got := snapshot()
			if got.Requests < 0 || got.Requests > count || got.BodyReadCalls < 0 || got.BodyReadCalls > count || got.AuthValidated < 0 || got.AuthValidated > count {
				t.Errorf("invalid concurrent snapshot: %+v", got)
			}
		}()
	}
	close(start)
	wg.Wait()
	if got := snapshot(); got.Requests != count || got.BodyReadCalls != count || got.AuthValidated != count {
		t.Fatalf("concurrent counters lost updates: %+v", got)
	}
	if atomic.LoadInt32(&emitted) != count {
		t.Fatalf("concurrent request evidence lost: emitted=%d", emitted)
	}
}

func eventObject(t *testing.T, e event) map[string]any {
	t.Helper()
	data, err := json.Marshal(e)
	if err != nil {
		t.Fatalf("event JSON: %v", err)
	}
	var value map[string]any
	if err := json.Unmarshal(data, &value); err != nil {
		t.Fatalf("event object: %v", err)
	}
	return value
}

func TestProtocolRetainsZeroAndFalseEvidence(t *testing.T) {
	for _, tc := range []struct {
		kind string
		want map[string]any
	}{
		{"request", map[string]any{"status": float64(0), "auth_valid": false, "body_read": false, "body_bytes": float64(0)}},
		{"consumer", map[string]any{"exit_code": float64(0), "signal": nil, "timed_out": false, "overflow": false, "stdout_b64": "", "stderr_b64": ""}},
		{"complete", map[string]any{"requests": float64(0), "body_read_calls": float64(0), "auth_validated": false, "provider_calls": float64(0), "listener_closure": false}},
	} {
		t.Run(tc.kind, func(t *testing.T) {
			value := eventObject(t, event{Type: tc.kind})
			for key, want := range tc.want {
				got, exists := value[key]
				if !exists || got != want {
					t.Errorf("required evidence %s: got=%#v exists=%t want=%#v", key, got, exists, want)
				}
			}
		})
	}
}

func recordTestListener(t *testing.T, address, state string) {
	t.Helper()
	path := os.Getenv("CC_LB_QA_LEDGER")
	if path == "" {
		return
	}
	entry := map[string]any{
		"kind": "owned-ephemeral-test-listener", "identifier": address,
		"owner_pid": os.Getpid(), "owner": t.Name(), "state": state, "owner_state": state,
		"created_at":     time.Now().UTC().Format(time.RFC3339Nano),
		"delete_command": "http.Server.Close in owning go test process",
	}
	data, err := json.Marshal(entry)
	if err != nil {
		t.Fatalf("listener ledger encoding: %v", err)
	}
	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		t.Fatalf("listener ledger open: %v", err)
	}
	_, writeErr := file.Write(append(data, '\n'))
	closeErr := file.Close()
	if writeErr != nil || closeErr != nil {
		t.Fatalf("listener ledger write=%v close=%v", writeErr, closeErr)
	}
}

func TestHTTPHeadersOnlyAuthRejectionDoesNotDrainBody(t *testing.T) {
	f := testFixture()
	h, snapshot := newHTTPHandler(testConfig("native-api-key"), f, nil, nil)
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("owned ephemeral listener: %v", err)
	}
	address := listener.Addr().String()
	t.Cleanup(func() {
		if err := listener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			t.Errorf("listener close: %v", err)
		}
		recordTestListener(t, address, "closed")
	})
	recordTestListener(t, address, "active")
	server := &http.Server{Handler: h, ReadHeaderTimeout: 2 * time.Second, ReadTimeout: 2 * time.Second, WriteTimeout: 2 * time.Second}
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	t.Cleanup(func() {
		if err := server.Close(); err != nil {
			t.Errorf("server close: %v", err)
		}
		select {
		case err := <-done:
			if !errors.Is(err, http.ErrServerClosed) && !errors.Is(err, net.ErrClosed) {
				t.Errorf("server exit: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Error("owned server did not terminate")
		}
	})
	conn, err := net.DialTimeout("tcp4", address, 2*time.Second)
	if err != nil {
		t.Fatalf("owned listener dial: %v", err)
	}
	defer conn.Close()
	if err := conn.SetDeadline(time.Now().Add(1500 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	// A small declared body would trigger net/http's automatic drain unless
	// the rejection explicitly closes the connection. No body is ever sent.
	headers := fmt.Sprintf("POST %s HTTP/1.1\r\nHost: %s\r\nContent-Length: 64\r\nX-Goog-Api-Key: wrong-synthetic-value\r\n\r\n", f.Request.Path, address)
	if _, err := io.WriteString(conn, headers); err != nil {
		t.Fatalf("headers write: %v", err)
	}
	reader := bufio.NewReader(conn)
	response, err := http.ReadResponse(reader, &http.Request{Method: http.MethodPost})
	if err != nil {
		t.Fatalf("auth response waited for an unsent body: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusUnauthorized && response.StatusCode != http.StatusForbidden {
		t.Fatalf("auth rejection status=%d", response.StatusCode)
	}
	if !response.Close {
		t.Fatal("headers-only rejection left connection reusable")
	}
	data, err := io.ReadAll(io.LimitReader(response.Body, 1024))
	if err != nil || bytes.Contains(data, []byte("QHARNESS_TEST_MARKER")) {
		t.Fatalf("auth response incomplete or replayed: err=%v", err)
	}
	if _, err := reader.ReadByte(); err != io.EOF {
		t.Fatalf("rejection did not close connection without draining: %v", err)
	}
	if got := snapshot(); got.BodyReadCalls != 0 || got.AuthValidated != 0 {
		t.Fatalf("headers-only rejected request produced body/auth evidence: %+v", got)
	}
}

func TestDecodeConfigRejectsMalformedAndUnsafeJSON(t *testing.T) {
	canonical, err := json.Marshal(testConfig("native-api-key"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := decodeConfig(bytes.NewReader(canonical)); err != nil {
		t.Fatalf("canonical config JSON rejected: %v", err)
	}
	if _, err := decodeConfig(strings.NewReader(string(canonical) + " \n\t")); err != nil {
		t.Fatalf("trailing JSON whitespace rejected: %v", err)
	}
	for _, tc := range []struct{ name, input string }{
		{"empty", ""},
		{"truncated", string(canonical[:len(canonical)-1])},
		{"null", "null"},
		{"array", "[]"},
		{"string", `"config"`},
		{"empty-object", "{}"},
		{"second-json-value", string(canonical) + "{}"},
		{"trailing-garbage", string(canonical) + "not-json"},
		{"unknown-field", `{"unrecognized":true,` + string(canonical[1:])},
		{"wrong-limit-type", strings.Replace(string(canonical), `"max_requests":32`, `"max_requests":"32"`, 1)},
		{"fractional-limit", strings.Replace(string(canonical), `"max_requests":32`, `"max_requests":1.5`, 1)},
		{"wrong-env-type", strings.Replace(string(canonical), `"HOME":"/home/qharness"`, `"HOME":false`, 1)},
		{"null-protocol", strings.Replace(string(canonical), `"protocol":"`+protocol+`"`, `"protocol":null`, 1)},
		{"oversized-whitespace", strings.Repeat(" ", maxConfig+1)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := decodeConfig(strings.NewReader(tc.input)); err == nil {
				t.Fatal("malformed or unsafe JSON config accepted")
			}
		})
	}
	for _, mutate := range []func(*config){
		func(c *config) { c.Limits.DeadlineMS = 999 },
		func(c *config) { c.Launch.Settings = nil },
		func(c *config) { c.Launch.Settings["telemetry"] = map[string]any{"enabled": true} },
		func(c *config) { c.Launch.Settings["unrecognized"] = true },
		func(c *config) { c.Launch.Env["GOOGLE_GENAI_USE_VERTEXAI"] = "true" },
		func(c *config) { c.Launch.Env["AGY_LLM_GATEWAY_API_KEY"] = "synthetic-gateway-api-key" },
	} {
		c := testConfig("native-api-key")
		mutate(&c)
		data, err := json.Marshal(c)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := decodeConfig(bytes.NewReader(data)); err == nil {
			t.Fatal("JSON decoder bypassed semantic launch validation")
		}
	}
	for _, mode := range []string{"native-api-key", "gateway-bearer"} {
		c := testConfig(mode)
		c.Limits = limits{MaxRequests: 1, MaxBytes: 1, DeadlineMS: 1000}
		if err := validate(c); err != nil {
			t.Fatalf("lower limit boundary rejected: %v", err)
		}
		data, err := json.Marshal(c)
		if err != nil {
			t.Fatal(err)
		}
		got, err := decodeConfig(bytes.NewReader(data))
		if err != nil || got.HostCanaryPort != c.HostCanaryPort || got.Client.BinarySHA256 != c.Client.BinarySHA256 {
			t.Fatalf("valid config decode lost pin/canary: %+v %v", got, err)
		}
	}
}

func TestValidateFixtureRejectsEmptyOrMalformedReplay(t *testing.T) {
	if err := validateFixture(testFixture()); err != nil {
		t.Fatalf("valid CRLF/LF SSE fixture rejected: %v", err)
	}
	cases := []struct {
		name   string
		mutate func(*fixture)
	}{
		{"no-method", func(f *fixture) { f.Request.Method = "" }},
		{"other-method", func(f *fixture) { f.Request.Method = http.MethodGet }},
		{"no-path", func(f *fixture) { f.Request.Path = "" }},
		{"absolute-url", func(f *fixture) { f.Request.Path = "http://127.0.0.1:18080/replay" }},
		{"scheme-relative-url", func(f *fixture) { f.Request.Path = "//example.invalid/replay" }},
		{"fragment", func(f *fixture) { f.Request.Path = "/replay#fragment" }},
		{"newline-path", func(f *fixture) { f.Request.Path = "/replay\r\nInjected: value" }},
		{"bad-percent-escape", func(f *fixture) { f.Request.Path = "/replay/%zz" }},
		{"nil-frames", func(f *fixture) { f.Response.Frames = nil }},
		{"empty-frames", func(f *fixture) { f.Response.Frames = []frame{} }},
		{"wrong-frame-kind", func(f *fixture) { f.Response.Frames[0].Kind = "ws" }},
		{"empty-frame-kind", func(f *fixture) { f.Response.Frames[0].Kind = "" }},
		{"empty-frame", func(f *fixture) { f.Response.Frames[0].Data = "" }},
		{"unterminated-frame", func(f *fixture) { f.Response.Frames[0].Data = "data: {}\n" }},
		{"raw-json-frame", func(f *fixture) { f.Response.Frames[0].Data = "{}" }},
		{"invalid-sse-line", func(f *fixture) { f.Response.Frames[0].Data = "not-an-sse-field\n\n" }},
		{"missing-data", func(f *fixture) { f.Response.Frames[0].Data = ": comment only\n\n" }},
		{"bare-carriage-return", func(f *fixture) { f.Response.Frames[0].Data = "data: {}\r\n\r" }},
		{"empty-marker", func(f *fixture) { f.Expected.Consumer.Marker = "" }},
		{"short-marker", func(f *fixture) { f.Expected.Consumer.Marker = "SHORT" }},
		{"oversized-marker", func(f *fixture) { f.Expected.Consumer.Marker = strings.Repeat("M", 101) }},
		{"lowercase-marker", func(f *fixture) { f.Expected.Consumer.Marker = "lowercase_marker" }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := testFixture()
			tc.mutate(&f)
			if err := validateFixture(f); err == nil {
				t.Fatal("empty or malformed fixture accepted")
			}
			data, err := json.Marshal(f)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := decodeFixture(bytes.NewReader(data)); err == nil {
				t.Fatal("fixture decoder bypassed semantic rejection")
			}
		})
	}
}

func TestDecodeFixtureKeepsRawFramesAndRejectsMalformedJSON(t *testing.T) {
	f := testFixture()
	data, err := json.Marshal(f)
	if err != nil {
		t.Fatal(err)
	}
	// Recorder metadata is independent of the replay fields. Unknown metadata
	// must not require reconstructing the raw SSE payload.
	metadata := `{"futureMetadata":{"unknown":true},` + string(data[1:])
	got, err := decodeFixture(strings.NewReader(metadata))
	if err != nil {
		t.Fatalf("valid fixture with recorder metadata rejected: %v", err)
	}
	if got.Request.Path != f.Request.Path || len(got.Response.Frames) != len(f.Response.Frames) {
		t.Fatalf("fixture URI/frame order lost: %+v", got)
	}
	for i, frame := range got.Response.Frames {
		if frame != f.Response.Frames[i] {
			t.Fatalf("frame %d rewritten: got=%q want=%q", i, frame.Data, f.Response.Frames[i].Data)
		}
	}
	for _, tc := range []struct{ name, input string }{
		{"empty", ""},
		{"null", "null"},
		{"array", "[]"},
		{"empty-object", "{}"},
		{"truncated", string(data[:len(data)-1])},
		{"trailing-json", string(data) + "{}"},
		{"wrong-frame-data-type", `{"request":{"method":"POST","path":"/replay"},"response":{"frames":[{"kind":"sse","data":{"text":"bad type"}}]},"expected":{"consumer":{"marker":"QHARNESS_TEST_MARKER"}}}`},
		{"wrong-frame-shape", `{"request":{"method":"POST","path":"/replay"},"response":{"frames":["data: {}\n\n"]},"expected":{"consumer":{"marker":"QHARNESS_TEST_MARKER"}}}`},
		{"oversized-json", strings.Repeat(" ", maxOutput+1)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := decodeFixture(strings.NewReader(tc.input)); err == nil {
				t.Fatal("malformed fixture JSON accepted")
			}
		})
	}
}

func TestDecodersEnforceByteCapsOnOtherwiseValidJSON(t *testing.T) {
	configJSON, err := json.Marshal(testConfig("native-api-key"))
	if err != nil {
		t.Fatal(err)
	}
	fixtureJSON, err := json.Marshal(testFixture())
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name   string
		data   []byte
		cap    int
		decode func(io.Reader) error
	}{
		{"config", configJSON, maxConfig, func(r io.Reader) error { _, err := decodeConfig(r); return err }},
		{"fixture", fixtureJSON, maxOutput, func(r io.Reader) error { _, err := decodeFixture(r); return err }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			atCap := append(append([]byte(nil), tc.data...), bytes.Repeat([]byte(" "), tc.cap-len(tc.data))...)
			if err := tc.decode(bytes.NewReader(atCap)); err != nil {
				t.Fatalf("valid JSON at byte cap rejected: %v", err)
			}
			overCap := append(atCap, ' ')
			if err := tc.decode(bytes.NewReader(overCap)); err == nil {
				t.Fatal("otherwise valid JSON over byte cap accepted")
			}
			body := &testBodySpy{reader: io.MultiReader(bytes.NewReader(overCap), strings.NewReader(strings.Repeat(" ", tc.cap)))}
			if err := tc.decode(body); err == nil {
				t.Fatal("oversized input accepted")
			}
			if body.bytes > tc.cap+1 {
				t.Fatalf("decoder read beyond bound: bytes=%d cap=%d", body.bytes, tc.cap)
			}
		})
	}
}

type testReadFailure struct{}

func (testReadFailure) Read([]byte) (int, error) {
	return 0, errors.New("untrusted-read-diagnostic")
}

func TestInputReadFailuresDoNotLeakDiagnostics(t *testing.T) {
	for _, decode := range []func(io.Reader) error{
		func(r io.Reader) error { _, err := decodeConfig(r); return err },
		func(r io.Reader) error { _, err := decodeFixture(r); return err },
	} {
		err := decode(testReadFailure{})
		if err == nil || strings.Contains(err.Error(), "untrusted-read-diagnostic") {
			t.Fatalf("input read failure did not return a static rejection: %v", err)
		}
	}
	f := testFixture()
	r, _ := requestWithSpy(f, "native-api-key", "")
	r.Body = &testBodySpy{reader: testReadFailure{}}
	r.ContentLength = -1
	h, _ := newHTTPHandler(testConfig("native-api-key"), f, nil, nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code < 400 || strings.Contains(w.Body.String(), "untrusted-read-diagnostic") || strings.Contains(w.Body.String(), "QHARNESS_TEST_MARKER") {
		t.Fatalf("unreadable body did not fail closed: status=%d response=%q", w.Code, w.Body.String())
	}
}

func TestReapAdoptedChildrenDrainsUntilNoChildren(t *testing.T) {
	type result struct {
		pid int
		err error
	}
	for _, tc := range []struct {
		name    string
		results []result
	}{
		{"already-empty", []result{{err: syscall.ECHILD}}},
		{"multiple-reaped", []result{{pid: 101}, {pid: 102}, {pid: 103}, {err: syscall.ECHILD}}},
		{"interrupted-then-reaped", []result{{err: syscall.EINTR}, {pid: 101}, {err: syscall.EINTR}, {pid: 102}, {err: syscall.ECHILD}}},
		{"temporarily-alive-then-reaped", []result{{pid: 0}, {pid: 101}, {pid: 0}, {pid: 102}, {err: syscall.ECHILD}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			wait := func() (int, error) {
				if calls >= len(tc.results) {
					t.Fatal("reaper continued after ECHILD")
				}
				next := tc.results[calls]
				calls++
				return next.pid, next.err
			}
			now := time.Unix(1700000000, 0)
			err := reapAdoptedChildren(wait, func() time.Time { return now }, now.Add(time.Second))
			if err != nil {
				t.Fatalf("adopted child sequence rejected: %v", err)
			}
			if calls != len(tc.results) {
				t.Fatalf("reaper returned before all children reached ECHILD: consumed=%d total=%d", calls, len(tc.results))
			}
		})
	}
}

func TestReapAdoptedChildrenDeadlineBoundsAliveOrInterruptedChildren(t *testing.T) {
	for _, tc := range []struct {
		name    string
		pid     int
		waitErr error
	}{
		{"still-alive", 0, nil},
		{"continuously-interrupted", 0, syscall.EINTR},
		{"continuous-reaped-children", 101, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			start := time.Unix(1700000000, 0)
			deadline := start.Add(4 * time.Millisecond)
			current := start
			var lastObservedTime time.Time
			now := func() time.Time {
				lastObservedTime = current
				current = current.Add(time.Millisecond)
				return lastObservedTime
			}
			calls := 0
			wait := func() (int, error) {
				calls++
				if calls > 4 {
					t.Fatal("reaper ignored advancing deadline")
				}
				if !lastObservedTime.Before(deadline) {
					t.Error("reaper attempted another wait after observing its deadline")
				}
				return tc.pid, tc.waitErr
			}
			err := reapAdoptedChildren(wait, now, deadline)
			if err == nil || err.Error() != "child_reap_timeout" {
				t.Fatalf("live children did not reach bounded static timeout: %v", err)
			}
			if calls == 0 {
				t.Fatal("deadline case never exercised nonblocking wait")
			}
		})
	}
}

func TestReapAdoptedChildrenExpiredDeadlineDoesNotWait(t *testing.T) {
	deadline := time.Unix(1700000000, 0)
	calls := 0
	err := reapAdoptedChildren(func() (int, error) {
		calls++
		return 0, syscall.ECHILD
	}, func() time.Time { return deadline }, deadline)
	if err == nil || err.Error() != "child_reap_timeout" || calls != 0 {
		t.Fatalf("expired deadline invoked wait or reported success: waits=%d error=%v", calls, err)
	}
}

func TestReapAdoptedChildrenWaitFailureIsStatic(t *testing.T) {
	for _, cause := range []error{syscall.EPERM, syscall.EINVAL, errors.New("untrusted-reap-diagnostic")} {
		now := time.Unix(1700000000, 0)
		calls := 0
		err := reapAdoptedChildren(func() (int, error) {
			calls++
			if calls > 1 {
				t.Fatal("failed wait was retried as a running child")
			}
			return 0, cause
		}, func() time.Time { return now }, now.Add(time.Second))
		if err == nil || err.Error() != "child_reap_failed" || calls != 1 {
			t.Fatalf("unexpected wait error was not rejected statically: calls=%d error=%v", calls, err)
		}
	}
}
