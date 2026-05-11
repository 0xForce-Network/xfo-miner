package scheduler

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/0xforce/xfo-miner/internal/debuglog"
	"github.com/ulikunitz/xz/lzma"
)

func TestDictionaryCacheHitSkipsRedownload(t *testing.T) {
	t.Parallel()

	body := mustLZMABytes(t, []byte("alpha\nbeta\ngamma\n"))
	checksum := sha256Hex(body)

	var hitCount int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(&hitCount, 1)
		_, _ = w.Write(body)
	}))
	defer ts.Close()

	baseDir := t.TempDir()
	cache := NewDictionaryCache(baseDir, ts.Client())
	spec := DictionaryCacheSpec{
		DictID:         "bt2024",
		DictURL:        ts.URL + "/dicts/bt2024.txt.lzma",
		CompressFormat: "lzma",
		Checksum:       checksum,
		LineCount:      8497528,
	}

	first, err := cache.Ensure(context.Background(), spec)
	if err != nil {
		t.Fatalf("first Ensure() error = %v", err)
	}
	if !first.Materialized {
		t.Fatalf("expected first Ensure() to materialize dictionary")
	}
	if got := atomic.LoadInt32(&hitCount); got != 1 {
		t.Fatalf("expected one download on first Ensure(), got %d", got)
	}

	metaPath := filepath.Join(baseDir, "bt2024.meta.json")
	if _, statErr := os.Stat(metaPath); statErr != nil {
		t.Fatalf("expected canonical metadata after materialization, stat err=%v", statErr)
	}

	txtPath := filepath.Join(baseDir, "bt2024.txt")
	if _, statErr := os.Stat(txtPath); statErr != nil {
		t.Fatalf("expected canonical txt after materialization, stat err=%v", statErr)
	}

	second, err := cache.Ensure(context.Background(), spec)
	if err != nil {
		t.Fatalf("second Ensure() error = %v", err)
	}
	if !second.Materialized {
		t.Fatalf("expected second Ensure() to hit materialized cache")
	}
	if got := atomic.LoadInt32(&hitCount); got != 1 {
		t.Fatalf("expected cache-hit Ensure() to skip redownload, got hits=%d", got)
	}
}

func TestDictionaryCacheDiskPreflightFailClosed(t *testing.T) {
	t.Parallel()

	body := mustLZMABytes(t, []byte("alpha\nbeta\n"))
	checksum := sha256Hex(body)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(body)
	}))
	defer ts.Close()

	baseDir := t.TempDir()
	cache := NewDictionaryCache(baseDir, ts.Client())
	cache.minAvailableBytes = 2 * 1024 * 1024 * 1024
	cache.statAvailableBytes = func(string) (uint64, error) {
		return 1024, nil
	}

	_, err := cache.Ensure(context.Background(), DictionaryCacheSpec{
		DictID:         "bt2024",
		DictURL:        ts.URL + "/dicts/bt2024.txt.lzma",
		CompressFormat: "lzma",
		Checksum:       checksum,
		LineCount:      2,
	})
	if err == nil || !isError(err, ErrDictionaryDiskSpaceInsufficient) {
		t.Fatalf("expected ErrDictionaryDiskSpaceInsufficient, got %v", err)
	}

	assertFileNotExists(t, filepath.Join(baseDir, "bt2024.txt"))
	assertFileNotExists(t, filepath.Join(baseDir, "bt2024.meta.json"))
	assertFileNotExists(t, filepath.Join(baseDir, "bt2024.lzma"))
	assertFileNotExists(t, filepath.Join(baseDir, ".tmp", "bt2024.download.tmp"))
	assertFileNotExists(t, filepath.Join(baseDir, ".tmp", "bt2024.extract.tmp"))
}

func TestDictionaryCacheExtractionQuotaExceededFailClosed(t *testing.T) {
	t.Parallel()

	plain := make([]byte, 0, 256)
	for i := 0; i < 128; i++ {
		plain = append(plain, 'a')
		plain = append(plain, '\n')
	}
	body := mustLZMABytes(t, plain)
	checksum := sha256Hex(body)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(body)
	}))
	defer ts.Close()

	baseDir := t.TempDir()
	cache := NewDictionaryCache(baseDir, ts.Client())
	cache.maxExtractBytes = 64
	cache.minAvailableBytes = 1
	cache.statAvailableBytes = func(string) (uint64, error) {
		return 10 * 1024 * 1024 * 1024, nil
	}

	_, err := cache.Ensure(context.Background(), DictionaryCacheSpec{
		DictID:         "bt2024",
		DictURL:        ts.URL + "/dicts/bt2024.txt.lzma",
		CompressFormat: "lzma",
		Checksum:       checksum,
		LineCount:      128,
	})
	if err == nil || !isError(err, ErrDictionarySizeQuotaExceeded) {
		t.Fatalf("expected ErrDictionarySizeQuotaExceeded, got %v", err)
	}

	assertFileNotExists(t, filepath.Join(baseDir, "bt2024.txt"))
	assertFileNotExists(t, filepath.Join(baseDir, "bt2024.meta.json"))
	assertFileNotExists(t, filepath.Join(baseDir, "bt2024.lzma"))
	assertFileNotExists(t, filepath.Join(baseDir, ".tmp", "bt2024.download.tmp"))
	assertFileNotExists(t, filepath.Join(baseDir, ".tmp", "bt2024.extract.tmp"))
}

func TestDictionaryCacheDefaultClientAllowsSlowDictionaryDownloads(t *testing.T) {
	t.Parallel()

	cache := NewDictionaryCache(t.TempDir(), nil)
	if cache.client == nil {
		t.Fatalf("expected default HTTP client")
	}
	if cache.client.Timeout < 15*time.Minute {
		t.Fatalf("expected dictionary HTTP timeout >= 15m, got %s", cache.client.Timeout)
	}
}

func TestDictionaryCacheStageLogsRequireDebugEnabled(t *testing.T) {
	if err := debuglog.Close(); err != nil {
		t.Fatalf("close pre-existing debug log: %v", err)
	}

	body := mustLZMABytes(t, []byte("alpha\nbeta\n"))
	checksum := sha256Hex(body)

	var hitCount int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(&hitCount, 1)
		_, _ = w.Write(body)
	}))
	defer ts.Close()

	cache := NewDictionaryCache(t.TempDir(), ts.Client())
	cache.minAvailableBytes = 1
	cache.statAvailableBytes = func(string) (uint64, error) {
		return 10 * 1024 * 1024 * 1024, nil
	}
	spec := DictionaryCacheSpec{
		DictID:         "bt2024",
		DictURL:        ts.URL + "/dicts/bt2024.txt.lzma",
		CompressFormat: "lzma",
		Checksum:       checksum,
		LineCount:      2,
	}

	if _, err := cache.Ensure(context.Background(), spec); err != nil {
		t.Fatalf("Ensure() without debug error = %v", err)
	}
	if debuglog.Enabled() {
		t.Fatalf("expected debug log to remain disabled")
	}

	debugPath := filepath.Join(t.TempDir(), "miner-debug.jsonl")
	if err := debuglog.Enable(debugPath, false); err != nil {
		t.Fatalf("enable debug log: %v", err)
	}
	t.Cleanup(func() {
		_ = debuglog.Close()
	})

	if _, err := cache.Ensure(context.Background(), spec); err != nil {
		t.Fatalf("Ensure() with debug cache hit error = %v", err)
	}
	if err := debuglog.Close(); err != nil {
		t.Fatalf("close debug log: %v", err)
	}

	raw, err := os.ReadFile(debugPath)
	if err != nil {
		t.Fatalf("read debug log: %v", err)
	}
	if !strings.Contains(string(raw), `"debug_event":"dictionary_cache_hit"`) {
		t.Fatalf("expected dictionary cache hit debug event, got %s", string(raw))
	}
	if got := atomic.LoadInt32(&hitCount); got != 1 {
		t.Fatalf("expected cache hit to avoid redownload, got hits=%d", got)
	}
}

func TestDictionaryCacheDownloadStageLogsWhenDebugEnabled(t *testing.T) {
	if err := debuglog.Close(); err != nil {
		t.Fatalf("close pre-existing debug log: %v", err)
	}

	body := mustLZMABytes(t, []byte("alpha\nbeta\ngamma\n"))
	checksum := sha256Hex(body)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(body)
	}))
	defer ts.Close()

	debugPath := filepath.Join(t.TempDir(), "miner-debug.jsonl")
	if err := debuglog.Enable(debugPath, false); err != nil {
		t.Fatalf("enable debug log: %v", err)
	}
	t.Cleanup(func() {
		_ = debuglog.Close()
	})

	cache := NewDictionaryCache(t.TempDir(), ts.Client())
	cache.minAvailableBytes = 1
	cache.statAvailableBytes = func(string) (uint64, error) {
		return 10 * 1024 * 1024 * 1024, nil
	}
	_, err := cache.Ensure(context.Background(), DictionaryCacheSpec{
		DictID:         "bt2024",
		DictURL:        ts.URL + "/dicts/bt2024.txt.lzma",
		CompressFormat: "lzma",
		Checksum:       checksum,
		LineCount:      3,
	})
	if err != nil {
		t.Fatalf("Ensure() with debug error = %v", err)
	}
	if err := debuglog.Close(); err != nil {
		t.Fatalf("close debug log: %v", err)
	}

	raw, err := os.ReadFile(debugPath)
	if err != nil {
		t.Fatalf("read debug log: %v", err)
	}
	logText := string(raw)
	for _, event := range []string{
		"dictionary_download_started",
		"dictionary_download_response_accepted",
		"dictionary_download_completed",
		"dictionary_checksum_started",
		"dictionary_checksum_completed",
		"dictionary_disk_preflight_started",
		"dictionary_disk_preflight_completed",
		"dictionary_extraction_started",
		"dictionary_extraction_completed",
		"dictionary_materialized",
	} {
		if !strings.Contains(logText, `"debug_event":"`+event+`"`) {
			t.Fatalf("expected debug event %s in log: %s", event, logText)
		}
	}
}

func mustLZMABytes(t *testing.T, plain []byte) []byte {
	t.Helper()

	tmpPath := filepath.Join(t.TempDir(), "dict.txt.lzma")
	f, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatalf("open lzma temp file: %v", err)
	}

	w, err := lzma.NewWriter(f)
	if err != nil {
		_ = f.Close()
		t.Fatalf("create lzma writer: %v", err)
	}
	if _, err := w.Write(plain); err != nil {
		_ = w.Close()
		_ = f.Close()
		t.Fatalf("write plain bytes to lzma: %v", err)
	}
	if err := w.Close(); err != nil {
		_ = f.Close()
		t.Fatalf("close lzma writer: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close lzma temp file: %v", err)
	}

	raw, err := os.ReadFile(tmpPath)
	if err != nil {
		t.Fatalf("read lzma temp file: %v", err)
	}
	return raw
}

func assertFileNotExists(t *testing.T, path string) {
	t.Helper()
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("expected file to not exist: %s err=%v", path, err)
	}
}

func isError(err error, target error) bool {
	return err != nil && target != nil && errors.Is(err, target)
}

func sha256Hex(input []byte) string {
	sum := sha256.Sum256(input)
	return hex.EncodeToString(sum[:])
}
