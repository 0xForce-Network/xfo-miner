package updater

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/0xforce/xfo-miner/internal/pool"
)

type Updater struct {
	logger         *slog.Logger
	client         *http.Client
	executablePath string
	args           []string
	environ        []string
	exitFunc       func(int)
	swapFn         func(context.Context, string) error
}

const oldBinarySuffix = ".old.tmp"

func New(logger *slog.Logger) (*Updater, error) {
	if logger == nil {
		logger = slog.Default()
	}

	execPath, err := os.Executable()
	if err != nil {
		return nil, fmt.Errorf("resolve executable path: %w", err)
	}
	execPath, err = filepath.Abs(execPath)
	if err != nil {
		return nil, fmt.Errorf("resolve absolute executable path: %w", err)
	}

	args := append([]string(nil), os.Args...)
	if len(args) == 0 {
		args = []string{execPath}
	} else {
		args[0] = execPath
	}

	u := &Updater{
		logger: logger,
		client: &http.Client{
			Timeout: 2 * time.Minute,
		},
		executablePath: execPath,
		args:           args,
		environ:        os.Environ(),
		exitFunc:       os.Exit,
	}
	u.swapFn = u.swapBinary

	return u, nil
}

func (u *Updater) Execute(ctx context.Context, ota *pool.OTAUpdateMessage) error {
	if ota == nil {
		return errors.New("ota message is nil")
	}
	if len(ota.DownloadURLs) == 0 {
		return errors.New("download_urls is empty")
	}

	wantChecksum, err := normalizeChecksum(ota.Checksum)
	if err != nil {
		return err
	}

	var attemptErrs []error
	for _, rawURL := range ota.DownloadURLs {
		parsed, err := url.Parse(rawURL)
		if err != nil {
			attemptErrs = append(attemptErrs, fmt.Errorf("invalid download url %q: %w", rawURL, err))
			continue
		}
		if parsed.Scheme != "https" {
			attemptErrs = append(attemptErrs, fmt.Errorf("non-https download url rejected: %s", rawURL))
			continue
		}

		tmpFile, gotChecksum, err := u.downloadAndHash(ctx, parsed.String())
		if err != nil {
			attemptErrs = append(attemptErrs, fmt.Errorf("download failed from %s: %w", parsed.String(), err))
			continue
		}

		if !strings.EqualFold(gotChecksum, wantChecksum) {
			_ = os.Remove(tmpFile)
			attemptErrs = append(attemptErrs, fmt.Errorf("checksum mismatch from %s: got %s want %s", parsed.String(), gotChecksum, wantChecksum))
			continue
		}

		artifactPath := tmpFile
		archiveFormat, err := detectArchiveFormat(parsed.Path, tmpFile)
		if err != nil {
			_ = os.Remove(tmpFile)
			attemptErrs = append(attemptErrs, fmt.Errorf("detect archive format from %s: %w", parsed.String(), err))
			continue
		}
		if archiveFormat != archiveFormatNone {
			extractDir, err := os.MkdirTemp(filepath.Dir(u.executablePath), ".xfo-miner-extract-*")
			if err != nil {
				_ = os.Remove(tmpFile)
				attemptErrs = append(attemptErrs, fmt.Errorf("create extract temp dir: %w", err))
				continue
			}
			if err := ExtractArchive(tmpFile, extractDir); err != nil {
				_ = os.Remove(tmpFile)
				_ = os.RemoveAll(extractDir)
				attemptErrs = append(attemptErrs, fmt.Errorf("extract archive from %s: %w", parsed.String(), err))
				continue
			}
			_ = os.Remove(tmpFile)

			artifactPath, err = locateExtractedBinary(extractDir, filepath.Base(u.executablePath))
			if err != nil {
				_ = os.RemoveAll(extractDir)
				attemptErrs = append(attemptErrs, fmt.Errorf("locate executable in archive from %s: %w", parsed.String(), err))
				continue
			}
		}

		u.logger.Info("OTA package verified", "url", parsed.String(), "latest_version", ota.LatestVersion)
		if err := u.swapFn(ctx, artifactPath); err != nil {
			_ = os.Remove(artifactPath)
			return fmt.Errorf("apply ota update: %w", err)
		}
		return nil
	}

	if len(attemptErrs) == 0 {
		return errors.New("no download attempts were executed")
	}
	return errors.Join(attemptErrs...)
}

func (u *Updater) downloadAndHash(ctx context.Context, downloadURL string) (string, string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, downloadURL, nil)
	if err != nil {
		return "", "", fmt.Errorf("create request: %w", err)
	}

	resp, err := u.client.Do(req)
	if err != nil {
		return "", "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", "", fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	workDir := filepath.Dir(u.executablePath)
	tmpFile, err := os.CreateTemp(workDir, ".xfo-miner-update-*")
	if err != nil {
		return "", "", fmt.Errorf("create temp file: %w", err)
	}
	tmpPath := tmpFile.Name()

	h := sha256.New()
	if _, err := io.Copy(io.MultiWriter(tmpFile, h), resp.Body); err != nil {
		_ = tmpFile.Close()
		_ = os.Remove(tmpPath)
		return "", "", fmt.Errorf("download body: %w", err)
	}

	if err := tmpFile.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return "", "", fmt.Errorf("close temp file: %w", err)
	}

	if err := os.Chmod(tmpPath, 0o755); err != nil {
		_ = os.Remove(tmpPath)
		return "", "", fmt.Errorf("chmod temp file: %w", err)
	}

	return tmpPath, hex.EncodeToString(h.Sum(nil)), nil
}

func normalizeChecksum(checksum string) (string, error) {
	trimmed := strings.ToLower(strings.TrimSpace(checksum))
	if len(trimmed) != 64 {
		return "", fmt.Errorf("invalid checksum length: got %d want 64", len(trimmed))
	}
	if _, err := hex.DecodeString(trimmed); err != nil {
		return "", fmt.Errorf("invalid checksum hex: %w", err)
	}
	return trimmed, nil
}

func oldBinaryPath(executablePath string) string {
	return executablePath + oldBinarySuffix
}