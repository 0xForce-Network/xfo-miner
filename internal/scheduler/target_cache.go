package scheduler

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"
)

var (
	ErrInvalidRemoteTargetContract = errors.New("invalid_remote_target_contract")
	ErrTargetDownloadFailed        = errors.New("target_download_failed")
	ErrTargetChecksumMismatch      = errors.New("target_checksum_mismatch")
	ErrTargetCacheWriteFailed      = errors.New("target_cache_write_failed")
)

var sha256Regex = regexp.MustCompile(`^[a-f0-9]{64}$`)

type RemoteTargetSpec struct {
	URL      string
	SHA256   string
	Filename string
}

type targetCache interface {
	Ensure(context.Context, RemoteTargetSpec) (string, error)
}

type TargetCache struct {
	baseDir string
	client  *http.Client
	mu      sync.Mutex
}

func NewTargetCache(baseDir string, client *http.Client) *TargetCache {
	if strings.TrimSpace(baseDir) == "" {
		baseDir = filepath.Join(os.TempDir(), "xfo-miner", "targets")
	}
	if client == nil {
		client = &http.Client{Timeout: 60 * time.Second}
	}
	return &TargetCache{baseDir: baseDir, client: client}
}

func (c *TargetCache) Ensure(ctx context.Context, spec RemoteTargetSpec) (string, error) {
	urlValue := strings.TrimSpace(spec.URL)
	shaValue := strings.ToLower(strings.TrimSpace(spec.SHA256))
	if urlValue == "" || !sha256Regex.MatchString(shaValue) {
		return "", ErrInvalidRemoteTargetContract
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if err := os.MkdirAll(c.baseDir, 0o755); err != nil {
		return "", fmt.Errorf("%w: %v", ErrTargetCacheWriteFailed, err)
	}

	localPath := c.pathFor(shaValue)
	if ok, _ := verifyFileSHA256(localPath, shaValue); ok {
		return localPath, nil
	}

	_ = os.Remove(localPath)
	tmpPath := localPath + ".tmp"
	_ = os.Remove(tmpPath)

	if err := c.downloadFile(ctx, urlValue, tmpPath); err != nil {
		_ = os.Remove(tmpPath)
		return "", fmt.Errorf("%w: %v", ErrTargetDownloadFailed, err)
	}

	ok, err := verifyFileSHA256(tmpPath, shaValue)
	if err != nil {
		_ = os.Remove(tmpPath)
		return "", fmt.Errorf("%w: %v", ErrTargetChecksumMismatch, err)
	}
	if !ok {
		_ = os.Remove(tmpPath)
		return "", ErrTargetChecksumMismatch
	}

	if err := os.Rename(tmpPath, localPath); err != nil {
		_ = os.Remove(tmpPath)
		return "", fmt.Errorf("%w: %v", ErrTargetCacheWriteFailed, err)
	}

	return localPath, nil
}

func (c *TargetCache) pathFor(sha256Value string) string {
	return filepath.Join(c.baseDir, sha256Value+".hc22000")
}

func (c *TargetCache) downloadFile(ctx context.Context, remoteURL string, outputPath string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, remoteURL, nil)
	if err != nil {
		return err
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	f, err := os.OpenFile(outputPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	defer f.Close()

	if _, err := io.Copy(f, resp.Body); err != nil {
		return err
	}
	return nil
}

func verifyFileSHA256(path string, expectedSHA256 string) (bool, error) {
	f, err := os.Open(path)
	if err != nil {
		return false, err
	}
	defer f.Close()

	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return false, err
	}

	actual := hex.EncodeToString(h.Sum(nil))
	return actual == strings.ToLower(strings.TrimSpace(expectedSHA256)), nil
}
