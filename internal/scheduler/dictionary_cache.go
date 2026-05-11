package scheduler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/0xforce/xfo-miner/internal/debuglog"
	"github.com/ulikunitz/xz/lzma"
)

var (
	ErrDictionaryDownloadFailed        = errors.New("dictionary_download_failed")
	ErrDictionaryChecksumMismatch      = errors.New("dictionary_checksum_mismatch")
	ErrDictionaryCacheWriteFailed      = errors.New("dictionary_cache_write_failed")
	ErrDictionaryDiskSpaceInsufficient = errors.New("dictionary_disk_space_insufficient")
	ErrDictionarySizeQuotaExceeded     = errors.New("dictionary_size_quota_exceeded")
	ErrDictionaryExtractFailed         = errors.New("dictionary_extract_failed")
)

const (
	defaultDictionaryMinAvailableBytes = uint64(2 * 1024 * 1024 * 1024)
	defaultDictionaryMaxExtractBytes   = int64(1024 * 1024 * 1024)
	defaultDictionaryHTTPClientTimeout = 15 * time.Minute
)

var errDictionaryExtractionQuotaExceeded = errors.New("dictionary_extraction_quota_exceeded")

type DictionaryCacheSpec struct {
	DictID         string
	DictURL        string
	CompressFormat string
	Checksum       string
	LineCount      int64
}

type DictionaryCacheResult struct {
	DictPath     string
	MetadataPath string
	BlobPath     string
	Materialized bool
}

type dictionaryCache interface {
	Ensure(context.Context, DictionaryCacheSpec) (DictionaryCacheResult, error)
}

type dictionaryCacheMetadata struct {
	DictID         string `json:"dict_id"`
	SourceURL      string `json:"source_url"`
	SourceSHA256   string `json:"source_sha256"`
	CompressFormat string `json:"compress_format"`
	LineCount      int64  `json:"line_count"`
	MaterializedAt int64  `json:"materialized_at"`
}

type DictionaryCache struct {
	baseDir            string
	client             *http.Client
	mu                 sync.Mutex
	statAvailableBytes func(string) (uint64, error)
	minAvailableBytes  uint64
	maxExtractBytes    int64
	logger             *slog.Logger
}

func NewDictionaryCache(baseDir string, client *http.Client) *DictionaryCache {
	if strings.TrimSpace(baseDir) == "" {
		baseDir = filepath.Join(os.TempDir(), "xfo-miner", "dicts")
	}
	if client == nil {
		client = &http.Client{Timeout: defaultDictionaryHTTPClientTimeout}
	}
	return &DictionaryCache{
		baseDir:            baseDir,
		client:             client,
		statAvailableBytes: statAvailableBytes,
		minAvailableBytes:  defaultDictionaryMinAvailableBytes,
		maxExtractBytes:    defaultDictionaryMaxExtractBytes,
		logger:             slog.Default(),
	}
}

func (c *DictionaryCache) Ensure(ctx context.Context, spec DictionaryCacheSpec) (DictionaryCacheResult, error) {
	normalized, err := normalizeDictionarySpec(spec)
	if err != nil {
		return DictionaryCacheResult{}, err
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if err := os.MkdirAll(c.baseDir, 0o755); err != nil {
		return DictionaryCacheResult{}, fmt.Errorf("%w: %v", ErrDictionaryCacheWriteFailed, err)
	}
	tmpDir := filepath.Join(c.baseDir, ".tmp")
	if err := os.MkdirAll(tmpDir, 0o755); err != nil {
		return DictionaryCacheResult{}, fmt.Errorf("%w: %v", ErrDictionaryCacheWriteFailed, err)
	}

	dictPath := filepath.Join(c.baseDir, normalized.DictID+".txt")
	metadataPath := filepath.Join(c.baseDir, normalized.DictID+".meta.json")
	blobPath := filepath.Join(c.baseDir, normalized.DictID+".lzma")

	if hit, hitErr := c.isMaterializedHit(dictPath, metadataPath, normalized); hitErr == nil && hit {
		c.logDebug("dictionary_cache_hit", "dict_id", normalized.DictID, "dict_path", dictPath, "metadata_path", metadataPath)
		return DictionaryCacheResult{DictPath: dictPath, MetadataPath: metadataPath, BlobPath: blobPath, Materialized: true}, nil
	}

	tmpBlobPath := filepath.Join(tmpDir, normalized.DictID+".download.tmp")
	tmpDictPath := filepath.Join(tmpDir, normalized.DictID+".extract.tmp")
	_ = os.Remove(tmpBlobPath)
	_ = os.Remove(tmpDictPath)
	c.logDebug("dictionary_download_started", "dict_id", normalized.DictID, "url", normalized.DictURL, "timeout", c.clientTimeoutString())
	downloadStartedAt := time.Now()
	downloadedBytes, err := c.downloadFile(ctx, normalized, tmpBlobPath)
	if err != nil {
		_ = os.Remove(tmpBlobPath)
		return DictionaryCacheResult{}, fmt.Errorf("%w: %v", ErrDictionaryDownloadFailed, err)
	}
	c.logDebug("dictionary_download_completed", "dict_id", normalized.DictID, "bytes", downloadedBytes, "duration_ms", time.Since(downloadStartedAt).Milliseconds())

	c.logDebug("dictionary_checksum_started", "dict_id", normalized.DictID, "checksum", normalized.Checksum)
	ok, verifyErr := verifyFileSHA256(tmpBlobPath, normalized.Checksum)
	if verifyErr != nil {
		_ = os.Remove(tmpBlobPath)
		return DictionaryCacheResult{}, fmt.Errorf("%w: %v", ErrDictionaryChecksumMismatch, verifyErr)
	}
	if !ok {
		_ = os.Remove(tmpBlobPath)
		return DictionaryCacheResult{}, ErrDictionaryChecksumMismatch
	}
	c.logDebug("dictionary_checksum_completed", "dict_id", normalized.DictID)

	c.logDebug("dictionary_disk_preflight_started", "dict_id", normalized.DictID, "base_dir", c.baseDir, "min_available_bytes", c.minAvailableBytes)
	if err := c.ensureDiskAvailable(c.baseDir); err != nil {
		_ = os.Remove(tmpBlobPath)
		return DictionaryCacheResult{}, err
	}
	c.logDebug("dictionary_disk_preflight_completed", "dict_id", normalized.DictID)

	c.logDebug("dictionary_extraction_started", "dict_id", normalized.DictID, "compress_format", normalized.CompressFormat, "max_extract_bytes", c.maxExtractBytes)
	extractStartedAt := time.Now()
	if err := c.extractLZMAToText(tmpBlobPath, tmpDictPath); err != nil {
		_ = os.Remove(tmpBlobPath)
		_ = os.Remove(tmpDictPath)
		return DictionaryCacheResult{}, err
	}
	c.logDebug("dictionary_extraction_completed", "dict_id", normalized.DictID, "duration_ms", time.Since(extractStartedAt).Milliseconds())

	_ = os.Remove(blobPath)
	if err := os.Rename(tmpBlobPath, blobPath); err != nil {
		_ = os.Remove(tmpBlobPath)
		_ = os.Remove(tmpDictPath)
		return DictionaryCacheResult{}, fmt.Errorf("%w: %v", ErrDictionaryCacheWriteFailed, err)
	}

	_ = os.Remove(dictPath)
	if err := os.Rename(tmpDictPath, dictPath); err != nil {
		_ = os.Remove(tmpDictPath)
		_ = os.Remove(blobPath)
		return DictionaryCacheResult{}, fmt.Errorf("%w: %v", ErrDictionaryCacheWriteFailed, err)
	}

	if err := writeDictionaryMetadataAtomically(metadataPath, dictionaryCacheMetadata{
		DictID:         normalized.DictID,
		SourceURL:      normalized.DictURL,
		SourceSHA256:   normalized.Checksum,
		CompressFormat: normalized.CompressFormat,
		LineCount:      normalized.LineCount,
		MaterializedAt: time.Now().Unix(),
	}); err != nil {
		_ = os.Remove(dictPath)
		_ = os.Remove(blobPath)
		_ = os.Remove(metadataPath)
		return DictionaryCacheResult{}, err
	}

	c.logDebug("dictionary_materialized", "dict_id", normalized.DictID, "dict_path", dictPath, "blob_path", blobPath, "metadata_path", metadataPath)
	return DictionaryCacheResult{DictPath: dictPath, MetadataPath: metadataPath, BlobPath: blobPath, Materialized: true}, nil
}

func (c *DictionaryCache) logDebug(event string, args ...any) {
	debuglog.Log(event, args...)
}

func (c *DictionaryCache) clientTimeoutString() string {
	if c == nil || c.client == nil || c.client.Timeout <= 0 {
		return "none"
	}
	return c.client.Timeout.String()
}

func (c *DictionaryCache) ensureDiskAvailable(path string) error {
	available, err := c.statAvailableBytes(path)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrDictionaryDiskSpaceInsufficient, err)
	}
	if available <= c.minAvailableBytes {
		return fmt.Errorf("%w: available=%d threshold=%d", ErrDictionaryDiskSpaceInsufficient, available, c.minAvailableBytes)
	}
	return nil
}

func (c *DictionaryCache) extractLZMAToText(blobPath string, outputPath string) error {
	blobFile, err := os.Open(blobPath)
	if err != nil {
		return fmt.Errorf("%w: open blob: %v", ErrDictionaryExtractFailed, err)
	}
	defer blobFile.Close()

	reader, err := lzma.NewReader(blobFile)
	if err != nil {
		return fmt.Errorf("%w: decoder init: %v", ErrDictionaryExtractFailed, err)
	}

	outFile, err := os.OpenFile(outputPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("%w: open output: %v", ErrDictionaryCacheWriteFailed, err)
	}

	quotaWriter := &boundedDictionaryWriter{writer: outFile, maxBytes: c.maxExtractBytes}
	_, copyErr := io.Copy(quotaWriter, reader)
	closeErr := outFile.Close()

	if errors.Is(copyErr, errDictionaryExtractionQuotaExceeded) {
		return fmt.Errorf("%w: max_bytes=%d", ErrDictionarySizeQuotaExceeded, c.maxExtractBytes)
	}
	if copyErr != nil {
		return fmt.Errorf("%w: decode copy failed: %v", ErrDictionaryExtractFailed, copyErr)
	}
	if closeErr != nil {
		return fmt.Errorf("%w: close output: %v", ErrDictionaryCacheWriteFailed, closeErr)
	}

	return nil
}

type boundedDictionaryWriter struct {
	writer   io.Writer
	written  int64
	maxBytes int64
}

func (w *boundedDictionaryWriter) Write(p []byte) (int, error) {
	if w.written >= w.maxBytes {
		return 0, errDictionaryExtractionQuotaExceeded
	}

	remaining := w.maxBytes - w.written
	if int64(len(p)) > remaining {
		n, err := w.writer.Write(p[:remaining])
		w.written += int64(n)
		if err != nil {
			return n, err
		}
		return n, errDictionaryExtractionQuotaExceeded
	}

	n, err := w.writer.Write(p)
	w.written += int64(n)
	return n, err
}

func normalizeDictionarySpec(spec DictionaryCacheSpec) (DictionaryCacheSpec, error) {
	normalized := DictionaryCacheSpec{
		DictID:         strings.TrimSpace(spec.DictID),
		DictURL:        strings.TrimSpace(spec.DictURL),
		CompressFormat: strings.ToLower(strings.TrimSpace(spec.CompressFormat)),
		Checksum:       strings.ToLower(strings.TrimSpace(spec.Checksum)),
		LineCount:      spec.LineCount,
	}

	if normalized.DictID == "" || normalized.DictURL == "" {
		return DictionaryCacheSpec{}, ErrInvalidDictionaryContract
	}
	if normalized.CompressFormat != "lzma" {
		return DictionaryCacheSpec{}, ErrUnsupportedDictionaryFormat
	}
	if !dictionaryChecksumSHA256Regex.MatchString(normalized.Checksum) {
		return DictionaryCacheSpec{}, ErrInvalidDictionaryContract
	}

	return normalized, nil
}

func (c *DictionaryCache) isMaterializedHit(dictPath string, metadataPath string, spec DictionaryCacheSpec) (bool, error) {
	if _, err := os.Stat(dictPath); err != nil {
		return false, err
	}

	raw, err := os.ReadFile(metadataPath)
	if err != nil {
		return false, err
	}

	var meta dictionaryCacheMetadata
	if err := json.Unmarshal(raw, &meta); err != nil {
		return false, err
	}

	if strings.TrimSpace(meta.DictID) != spec.DictID {
		return false, nil
	}
	if strings.TrimSpace(meta.SourceURL) != spec.DictURL {
		return false, nil
	}
	if strings.ToLower(strings.TrimSpace(meta.SourceSHA256)) != spec.Checksum {
		return false, nil
	}
	if strings.ToLower(strings.TrimSpace(meta.CompressFormat)) != spec.CompressFormat {
		return false, nil
	}
	if meta.LineCount != spec.LineCount {
		return false, nil
	}

	return true, nil
}

func writeDictionaryMetadataAtomically(path string, metadata dictionaryCacheMetadata) error {
	raw, err := json.Marshal(metadata)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrDictionaryCacheWriteFailed, err)
	}

	tmpPath := path + ".tmp"
	_ = os.Remove(tmpPath)
	if err := os.WriteFile(tmpPath, raw, 0o600); err != nil {
		return fmt.Errorf("%w: %v", ErrDictionaryCacheWriteFailed, err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("%w: %v", ErrDictionaryCacheWriteFailed, err)
	}

	return nil
}

func (c *DictionaryCache) downloadFile(ctx context.Context, spec DictionaryCacheSpec, outputPath string) (int64, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, spec.DictURL, nil)
	if err != nil {
		return 0, err
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return 0, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}
	c.logDebug("dictionary_download_response_accepted", "dict_id", spec.DictID, "url", spec.DictURL, "status_code", resp.StatusCode, "content_length", resp.ContentLength)

	f, err := os.OpenFile(outputPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	written, err := io.Copy(f, resp.Body)
	if err != nil {
		return written, err
	}
	return written, nil
}
