package scheduler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

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
}

func NewDictionaryCache(baseDir string, client *http.Client) *DictionaryCache {
	if strings.TrimSpace(baseDir) == "" {
		baseDir = filepath.Join(os.TempDir(), "xfo-miner", "dicts")
	}
	if client == nil {
		client = &http.Client{Timeout: 60 * time.Second}
	}
	return &DictionaryCache{
		baseDir:            baseDir,
		client:             client,
		statAvailableBytes: statAvailableBytes,
		minAvailableBytes:  defaultDictionaryMinAvailableBytes,
		maxExtractBytes:    defaultDictionaryMaxExtractBytes,
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
		return DictionaryCacheResult{DictPath: dictPath, MetadataPath: metadataPath, BlobPath: blobPath, Materialized: true}, nil
	}

	tmpBlobPath := filepath.Join(tmpDir, normalized.DictID+".download.tmp")
	tmpDictPath := filepath.Join(tmpDir, normalized.DictID+".extract.tmp")
	_ = os.Remove(tmpBlobPath)
	_ = os.Remove(tmpDictPath)
	if err := c.downloadFile(ctx, normalized.DictURL, tmpBlobPath); err != nil {
		_ = os.Remove(tmpBlobPath)
		return DictionaryCacheResult{}, fmt.Errorf("%w: %v", ErrDictionaryDownloadFailed, err)
	}

	ok, verifyErr := verifyFileSHA256(tmpBlobPath, normalized.Checksum)
	if verifyErr != nil {
		_ = os.Remove(tmpBlobPath)
		return DictionaryCacheResult{}, fmt.Errorf("%w: %v", ErrDictionaryChecksumMismatch, verifyErr)
	}
	if !ok {
		_ = os.Remove(tmpBlobPath)
		return DictionaryCacheResult{}, ErrDictionaryChecksumMismatch
	}

	if err := c.ensureDiskAvailable(c.baseDir); err != nil {
		_ = os.Remove(tmpBlobPath)
		return DictionaryCacheResult{}, err
	}

	if err := c.extractLZMAToText(tmpBlobPath, tmpDictPath); err != nil {
		_ = os.Remove(tmpBlobPath)
		_ = os.Remove(tmpDictPath)
		return DictionaryCacheResult{}, err
	}

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

	return DictionaryCacheResult{DictPath: dictPath, MetadataPath: metadataPath, BlobPath: blobPath, Materialized: true}, nil
}

func statAvailableBytes(path string) (uint64, error) {
	var fs syscall.Statfs_t
	if err := syscall.Statfs(path, &fs); err != nil {
		return 0, err
	}
	return fs.Bavail * uint64(fs.Bsize), nil
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

func (c *DictionaryCache) downloadFile(ctx context.Context, remoteURL string, outputPath string) error {
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
