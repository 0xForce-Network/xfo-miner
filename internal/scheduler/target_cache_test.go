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
	"testing"
)

func TestTargetCacheEnsureDownloadsVerifiesAndReuses(t *testing.T) {
	content := []byte("WPA*02*504-sample")
	sum := sha256.Sum256(content)
	expectedSHA := hex.EncodeToString(sum[:])

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(content)
	}))
	defer server.Close()

	cacheDir := t.TempDir()
	cache := NewTargetCache(cacheDir, server.Client())

	resolvedPath, err := cache.Ensure(context.Background(), RemoteTargetSpec{
		URL:    server.URL,
		SHA256: expectedSHA,
	})
	if err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}

	if filepath.Dir(resolvedPath) != cacheDir {
		t.Fatalf("expected cache dir %q, got %q", cacheDir, filepath.Dir(resolvedPath))
	}
	raw, err := os.ReadFile(resolvedPath)
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	if string(raw) != string(content) {
		t.Fatalf("cached content mismatch: got %q want %q", string(raw), string(content))
	}

	server.Close()
	reusedPath, err := cache.Ensure(context.Background(), RemoteTargetSpec{
		URL:    server.URL,
		SHA256: expectedSHA,
	})
	if err != nil {
		t.Fatalf("Ensure() cache reuse error = %v", err)
	}
	if reusedPath != resolvedPath {
		t.Fatalf("expected same cache path, got %q vs %q", reusedPath, resolvedPath)
	}
}

func TestTargetCacheEnsureChecksumMismatch(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("wrong-artifact"))
	}))
	defer server.Close()

	cache := NewTargetCache(t.TempDir(), server.Client())
	_, err := cache.Ensure(context.Background(), RemoteTargetSpec{
		URL:    server.URL,
		SHA256: "8930027d3b0e5082df6405092d63dc04d6880ed6da3616c3793c5dc785e0801b",
	})
	if !errors.Is(err, ErrTargetChecksumMismatch) {
		t.Fatalf("expected ErrTargetChecksumMismatch, got %v", err)
	}
}

func TestTargetCacheEnsureInvalidContract(t *testing.T) {
	cache := NewTargetCache(t.TempDir(), nil)
	_, err := cache.Ensure(context.Background(), RemoteTargetSpec{
		URL:    "https://pool.local/artifacts/504.hc22000",
		SHA256: "not-a-sha256",
	})
	if !errors.Is(err, ErrInvalidRemoteTargetContract) {
		t.Fatalf("expected ErrInvalidRemoteTargetContract, got %v", err)
	}
}
