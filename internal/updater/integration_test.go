package updater

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"runtime"
	"sync"
	"testing"

	"github.com/0xforce/xfo-miner/internal/pool"
)

func buildTarGzBinary(t *testing.T, path string, payload []byte) []byte {
	t.Helper()

	buf := bytes.NewBuffer(nil)
	gzw := gzip.NewWriter(buf)
	tw := tar.NewWriter(gzw)
	h := &tar.Header{Name: path, Mode: 0o755, Size: int64(len(payload))}
	if err := tw.WriteHeader(h); err != nil {
		t.Fatalf("write tar header: %v", err)
	}
	if _, err := tw.Write(payload); err != nil {
		t.Fatalf("write tar payload: %v", err)
	}
	if err := tw.Close(); err != nil {
		t.Fatalf("close tar writer: %v", err)
	}
	if err := gzw.Close(); err != nil {
		t.Fatalf("close gzip writer: %v", err)
	}

	return buf.Bytes()
}

func TestIntegrationPollerToUpdaterTarGzFlow(t *testing.T) {
	t.Parallel()

	payload := []byte("new-binary-from-cdn")
	archiveBytes := buildTarGzBinary(t, "release/"+packagedBinaryName(runtime.GOOS, runtime.GOARCH), payload)
	checksum := sha256.Sum256(archiveBytes)
	platformKey := runtime.GOOS + "-" + runtime.GOARCH

	var server *httptest.Server
	server = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/releases/latest.json":
			_, _ = fmt.Fprintf(w, `{"latest_version":"0.2.0","min_version":"0.1.0","release_notes":"integration","assets":{"%s":{"download_url":"%s/releases/v0.2.0/xfo-miner-%s.tar.gz","checksum":"%s","filename":"xfo-miner-%s.tar.gz"}}}`,
				platformKey, server.URL, platformKey, hex.EncodeToString(checksum[:]), platformKey)
		case "/releases/v0.2.0/xfo-miner-" + platformKey + ".tar.gz":
			_, _ = w.Write(archiveBytes)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	up := testUpdater(t)
	up.client = server.Client()

	var (
		mu     sync.Mutex
		called int
	)
	up.swapFn = func(_ context.Context, downloadedFile string) error {
		mu.Lock()
		defer mu.Unlock()
		called++

		got, err := os.ReadFile(downloadedFile)
		if err != nil {
			return err
		}
		if string(got) != string(payload) {
			return fmt.Errorf("unexpected extracted payload: got %q want %q", string(got), string(payload))
		}
		return os.Remove(downloadedFile)
	}

	base, err := ParseVersion("0.1.0")
	if err != nil {
		t.Fatalf("parse base version: %v", err)
	}

	poller := NewPoller(
		server.URL+"/releases/latest.json",
		0,
		0,
		base,
		server.Client(),
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		func(ctx context.Context, ota *pool.OTAUpdateMessage) error {
			return up.Execute(ctx, ota)
		},
	)

	if err := poller.CheckOnce(context.Background()); err != nil {
		t.Fatalf("CheckOnce() error = %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if called != 1 {
		t.Fatalf("expected updater swap called once, got %d", called)
	}
}

func TestIntegrationPassiveOTAUpdateExecuteFlow(t *testing.T) {
	t.Parallel()

	payload := []byte("passive-update-binary")
	archiveBytes := buildTarGzBinary(t, "release/"+packagedBinaryName(runtime.GOOS, runtime.GOARCH), payload)
	checksum := sha256.Sum256(archiveBytes)

	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(archiveBytes)
	}))
	defer server.Close()

	up := testUpdater(t)
	up.client = server.Client()

	called := 0
	up.swapFn = func(_ context.Context, downloadedFile string) error {
		called++
		got, err := os.ReadFile(downloadedFile)
		if err != nil {
			return err
		}
		if string(got) != string(payload) {
			return fmt.Errorf("unexpected extracted payload: got %q want %q", string(got), string(payload))
		}
		return os.Remove(downloadedFile)
	}

	err := up.Execute(context.Background(), &pool.OTAUpdateMessage{
		Type:          "update_required",
		LatestVersion: "0.2.0",
		DownloadURLs:  []string{server.URL + "/xfo-miner.tar.gz"},
		Checksum:      hex.EncodeToString(checksum[:]),
	})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if called != 1 {
		t.Fatalf("expected swap called once, got %d", called)
	}
}

func TestIntegrationPollerSkipsWhenVersionMatches(t *testing.T) {
	t.Parallel()

	platformKey := runtime.GOOS + "-" + runtime.GOARCH
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprintf(w, `{"latest_version":"0.2.0","min_version":"0.1.0","release_notes":"n/a","assets":{"%s":{"download_url":"https://update.xfo.network/releases/v0.2.0/xfo-miner.tar.gz","checksum":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","filename":"xfo-miner.tar.gz"}}}`,
			platformKey)
	}))
	defer server.Close()

	base, err := ParseVersion("0.2.0")
	if err != nil {
		t.Fatalf("parse base version: %v", err)
	}

	called := 0
	poller := NewPoller(server.URL, 0, 0, base, server.Client(), slog.New(slog.NewTextHandler(io.Discard, nil)), func(_ context.Context, _ *pool.OTAUpdateMessage) error {
		called++
		return nil
	})

	if err := poller.CheckOnce(context.Background()); err != nil {
		t.Fatalf("CheckOnce() error = %v", err)
	}
	if called != 0 {
		t.Fatalf("expected no update callback when versions match, got %d", called)
	}
}

func TestIntegrationPollerCDNUnavailable(t *testing.T) {
	t.Parallel()

	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "temporary unavailable", http.StatusServiceUnavailable)
	}))
	defer server.Close()

	base, err := ParseVersion("0.1.0")
	if err != nil {
		t.Fatalf("parse base version: %v", err)
	}

	poller := NewPoller(server.URL, 0, 0, base, server.Client(), slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
	if err := poller.CheckOnce(context.Background()); err == nil {
		t.Fatalf("expected CheckOnce() error on CDN unavailable")
	}
}
