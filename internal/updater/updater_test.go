package updater

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/0xforce/xfo-miner/internal/pool"
)

func testUpdater(t *testing.T) *Updater {
	t.Helper()
	return testUpdaterWithExecutableName(t, "xfo-miner")
	}

func testUpdaterWithExecutableName(t *testing.T, executableName string) *Updater {
	t.Helper()

	tmpDir := t.TempDir()
	if strings.TrimSpace(executableName) == "" {
		executableName = "xfo-miner"
	}
	execPath := filepath.Join(tmpDir, executableName)
	if err := os.WriteFile(execPath, []byte("old-binary"), 0o755); err != nil {
		t.Fatalf("write fake executable: %v", err)
	}

	u := &Updater{
		logger:         slog.New(slog.NewTextHandler(io.Discard, nil)),
		client:         &http.Client{},
		executablePath: execPath,
		args:           []string{execPath},
		environ:        []string{},
		exitFunc:       func(int) {},
	}
	u.swapFn = func(_ context.Context, downloadedFile string) error {
		return os.Remove(downloadedFile)
	}

	return u
}

func TestExecuteArchiveUpdatePreservesCustomExecutablePath(t *testing.T) {
	t.Parallel()

	customExecName := "rig-alpha"
	archiveBinaryName := "xfo-miner"
	if runtime.GOOS == "windows" {
		customExecName += ".exe"
		archiveBinaryName += ".exe"
	}

	payload := []byte("custom-name-updated-binary")
	archiveBytes := buildTarGzBinary(t, "release/"+archiveBinaryName, payload)
	checksum := sha256.Sum256(archiveBytes)

	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(archiveBytes)
	}))
	defer server.Close()

	u := testUpdaterWithExecutableName(t, customExecName)
	u.client = server.Client()

	swapCalled := 0
	u.swapFn = func(_ context.Context, downloadedFile string) error {
		swapCalled++
		if filepath.Base(u.executablePath) != customExecName {
			t.Fatalf("expected custom executable basename %q, got %q", customExecName, filepath.Base(u.executablePath))
		}
		if len(u.args) == 0 || u.args[0] != u.executablePath {
			t.Fatalf("expected args[0] to preserve executable path %q, got %v", u.executablePath, u.args)
		}

		got, err := os.ReadFile(downloadedFile)
		if err != nil {
			return err
		}
		if string(got) != string(payload) {
			t.Fatalf("unexpected extracted payload: got %q want %q", string(got), string(payload))
		}

		if err := os.Remove(u.executablePath); err != nil {
			return err
		}
		if err := os.Rename(downloadedFile, u.executablePath); err != nil {
			return err
		}

		replaced, err := os.ReadFile(u.executablePath)
		if err != nil {
			return err
		}
		if string(replaced) != string(payload) {
			t.Fatalf("unexpected replaced executable payload: got %q want %q", string(replaced), string(payload))
		}
		return nil
	}

	err := u.Execute(context.Background(), &pool.OTAUpdateMessage{
		LatestVersion: "0.2.0",
		DownloadURLs:  []string{server.URL + "/pkg.tar.gz"},
		Checksum:      hex.EncodeToString(checksum[:]),
	})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if swapCalled != 1 {
		t.Fatalf("expected swap called once, got %d", swapCalled)
	}
}

func TestExecuteFallbackAndChecksumSuccess(t *testing.T) {
	t.Parallel()

	payload := []byte("new-binary")
	checksum := sha256.Sum256(payload)

	primary := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
	}))
	defer primary.Close()

	fallback := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(payload)
	}))
	defer fallback.Close()

	u := testUpdater(t)
	u.client = fallback.Client()

	called := 0
	u.swapFn = func(_ context.Context, downloadedFile string) error {
		called++
		if _, err := os.Stat(downloadedFile); err != nil {
			t.Fatalf("expected downloaded file to exist: %v", err)
		}
		return os.Remove(downloadedFile)
	}

	err := u.Execute(context.Background(), &pool.OTAUpdateMessage{
		LatestVersion: "0.2.0",
		DownloadURLs:  []string{primary.URL + "/pkg", fallback.URL + "/pkg"},
		Checksum:      hex.EncodeToString(checksum[:]),
	})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if called != 1 {
		t.Fatalf("expected swap to be called once, got %d", called)
	}
}

func TestExecuteChecksumMismatch(t *testing.T) {
	t.Parallel()

	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("tampered"))
	}))
	defer server.Close()

	u := testUpdater(t)
	u.client = server.Client()

	swapCalled := false
	u.swapFn = func(_ context.Context, downloadedFile string) error {
		swapCalled = true
		return errors.New("unexpected swap call")
	}

	err := u.Execute(context.Background(), &pool.OTAUpdateMessage{
		LatestVersion: "0.2.0",
		DownloadURLs:  []string{server.URL + "/pkg"},
		Checksum:      strings.Repeat("a", 64),
	})
	if err == nil {
		t.Fatalf("expected checksum mismatch error")
	}
	if !strings.Contains(err.Error(), "checksum mismatch") {
		t.Fatalf("expected checksum mismatch error, got %v", err)
	}
	if swapCalled {
		t.Fatalf("swap should not be called on checksum mismatch")
	}
}

func TestExecuteRejectsNonHTTPSURL(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("binary"))
	}))
	defer server.Close()

	u := testUpdater(t)
	err := u.Execute(context.Background(), &pool.OTAUpdateMessage{
		LatestVersion: "0.2.0",
		DownloadURLs:  []string{server.URL + "/pkg"},
		Checksum:      strings.Repeat("b", 64),
	})
	if err == nil {
		t.Fatalf("expected https enforcement error")
	}
	if !strings.Contains(err.Error(), "non-https") {
		t.Fatalf("expected non-https rejection, got %v", err)
	}
}