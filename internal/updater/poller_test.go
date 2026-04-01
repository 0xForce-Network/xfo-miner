package updater

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"runtime"
	"sync"
	"testing"

	"github.com/0xforce/xfo-miner/internal/pool"
)

func TestPollerCheckOnceTriggersUpdateWhenNewerVersion(t *testing.T) {
	t.Parallel()

	platformKey := runtime.GOOS + "-" + runtime.GOARCH
	manifest := fmt.Sprintf(`{
		"latest_version":"0.2.0",
		"min_version":"0.1.0",
		"release_notes":"notes",
		"assets":{
			%q:{
				"download_url":"https://update.xfo.network/releases/v0.2.0/xfo-miner.tar.gz",
				"checksum":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
				"filename":"xfo-miner.tar.gz"
			}
		}
	}`, platformKey)

	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(manifest))
	}))
	defer server.Close()

	base, err := ParseVersion("0.1.0")
	if err != nil {
		t.Fatalf("parse base version: %v", err)
	}

	var (
		mu     sync.Mutex
		called int
		got    *pool.OTAUpdateMessage
	)

	poller := NewPoller(server.URL, 0, 0, base, server.Client(), slog.New(slog.NewTextHandler(io.Discard, nil)), func(_ context.Context, ota *pool.OTAUpdateMessage) error {
		mu.Lock()
		defer mu.Unlock()
		called++
		if ota != nil {
			cp := *ota
			got = &cp
		}
		return nil
	})

	if err := poller.CheckOnce(context.Background()); err != nil {
		t.Fatalf("CheckOnce() error = %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if called != 1 {
		t.Fatalf("expected onUpdate called once, got %d", called)
	}
	if got == nil {
		t.Fatalf("expected ota payload")
	}
	if got.LatestVersion != "0.2.0" {
		t.Fatalf("unexpected latest version: %q", got.LatestVersion)
	}
	if len(got.DownloadURLs) != 1 {
		t.Fatalf("unexpected download urls: %#v", got.DownloadURLs)
	}
}

func TestPollerCheckOnceSkipsWhenSameVersion(t *testing.T) {
	t.Parallel()

	platformKey := runtime.GOOS + "-" + runtime.GOARCH
	manifest := fmt.Sprintf(`{
		"latest_version":"0.2.0",
		"min_version":"0.1.0",
		"release_notes":"notes",
		"assets":{
			%q:{
				"download_url":"https://update.xfo.network/releases/v0.2.0/xfo-miner.tar.gz",
				"checksum":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
				"filename":"xfo-miner.tar.gz"
			}
		}
	}`, platformKey)

	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(manifest))
	}))
	defer server.Close()

	base, err := ParseVersion("0.2.0")
	if err != nil {
		t.Fatalf("parse base version: %v", err)
	}

	called := 0
	poller := NewPoller(server.URL, 0, 0, base, server.Client(), slog.New(slog.NewTextHandler(io.Discard, nil)), func(_ context.Context, ota *pool.OTAUpdateMessage) error {
		called++
		return nil
	})

	if err := poller.CheckOnce(context.Background()); err != nil {
		t.Fatalf("CheckOnce() error = %v", err)
	}
	if called != 0 {
		t.Fatalf("expected onUpdate to be skipped, got %d calls", called)
	}
}

func TestPollerCheckOnceHTTPError(t *testing.T) {
	t.Parallel()

	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	defer server.Close()

	base, err := ParseVersion("0.1.0")
	if err != nil {
		t.Fatalf("parse base version: %v", err)
	}

	poller := NewPoller(server.URL, 0, 0, base, server.Client(), slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
	if err := poller.CheckOnce(context.Background()); err == nil {
		t.Fatalf("expected CheckOnce() to fail on HTTP 500")
	}
}
