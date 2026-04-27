package updater

import (
	"archive/tar"
	"archive/zip"
	"bytes"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"strings"
)

const (
	archiveFormatNone  = ""
	archiveFormatTarGz = "tar.gz"
	archiveFormatZip   = "zip"
)

func ExtractArchive(archivePath, destDir string) error {
	if strings.TrimSpace(archivePath) == "" {
		return errors.New("archive path is empty")
	}
	if strings.TrimSpace(destDir) == "" {
		return errors.New("destination directory is empty")
	}

	if err := os.MkdirAll(destDir, 0o755); err != nil {
		return fmt.Errorf("create destination directory: %w", err)
	}

	format, err := detectArchiveFormat(archivePath, archivePath)
	if err != nil {
		return err
	}

	switch format {
	case archiveFormatTarGz:
		return extractTarGz(archivePath, destDir)
	case archiveFormatZip:
		return extractZip(archivePath, destDir)
	default:
		return fmt.Errorf("unsupported archive format: %q", format)
	}
}

func detectArchiveFormat(sourceHint, filePath string) (string, error) {
	lower := strings.ToLower(strings.TrimSpace(sourceHint))
	if strings.HasSuffix(lower, ".tar.gz") || strings.HasSuffix(lower, ".tgz") {
		return archiveFormatTarGz, nil
	}
	if strings.HasSuffix(lower, ".zip") {
		return archiveFormatZip, nil
	}

	f, err := os.Open(filePath)
	if err != nil {
		return archiveFormatNone, fmt.Errorf("open file for archive detection: %w", err)
	}
	defer f.Close()

	header := make([]byte, 4)
	n, err := io.ReadFull(f, header)
	if err != nil && !errors.Is(err, io.ErrUnexpectedEOF) {
		return archiveFormatNone, fmt.Errorf("read file header for archive detection: %w", err)
	}
	header = header[:n]

	if len(header) >= 2 && header[0] == 0x1f && header[1] == 0x8b {
		return archiveFormatTarGz, nil
	}
	if len(header) >= 4 && bytes.Equal(header[:4], []byte("PK\x03\x04")) {
		return archiveFormatZip, nil
	}

	return archiveFormatNone, nil
}

func locateExtractedBinary(destDir, preferredName string) (string, error) {
	candidates := extractedBinaryCandidates(preferredName)

	seen := map[string]struct{}{}
	for _, c := range candidates {
		c = strings.TrimSpace(c)
		if c == "" {
			continue
		}
		if _, ok := seen[c]; ok {
			continue
		}
		seen[c] = struct{}{}

		var found string
		err := filepath.WalkDir(destDir, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() {
				return nil
			}
			if strings.EqualFold(filepath.Base(path), c) {
				found = path
				return io.EOF
			}
			return nil
		})
		if err != nil && !errors.Is(err, io.EOF) {
			return "", fmt.Errorf("scan extracted files: %w", err)
		}
		if found != "" {
			if err := os.Chmod(found, 0o755); err != nil {
				return "", fmt.Errorf("chmod extracted binary: %w", err)
			}
			return found, nil
		}
	}

	return "", errors.New("main binary not found in extracted archive")
}

func extractedBinaryCandidates(preferredName string) []string {
	candidates := []string{}
	if strings.TrimSpace(preferredName) != "" {
		candidates = append(candidates, preferredName)
	}
	candidates = append(candidates,
		packagedBinaryName(runtime.GOOS, runtime.GOARCH),
		"xfo-miner",
		"xfo-miner.exe",
	)
	return candidates
}

func packagedBinaryName(goos, goarch string) string {
	name := fmt.Sprintf("xfo-miner-%s-%s", goos, goarch)
	if goos == "windows" {
		return name + ".exe"
	}
	return name
}

func extractTarGz(archivePath, destDir string) error {
	f, err := os.Open(archivePath)
	if err != nil {
		return fmt.Errorf("open tar.gz: %w", err)
	}
	defer f.Close()

	gz, err := gzip.NewReader(f)
	if err != nil {
		return fmt.Errorf("open gzip stream: %w", err)
	}
	defer gz.Close()

	tr := tar.NewReader(gz)
	for {
		hdr, err := tr.Next()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("read tar entry: %w", err)
		}

		target, err := safeJoin(destDir, hdr.Name)
		if err != nil {
			return err
		}

		switch hdr.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(target, 0o755); err != nil {
				return fmt.Errorf("create directory %q: %w", target, err)
			}
		case tar.TypeReg, tar.TypeRegA:
			if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
				return fmt.Errorf("create parent directory for %q: %w", target, err)
			}
			perm := fileModeOrDefault(fs.FileMode(hdr.Mode), 0o644)
			out, err := os.OpenFile(target, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, perm)
			if err != nil {
				return fmt.Errorf("create file %q: %w", target, err)
			}
			if _, err := io.Copy(out, tr); err != nil {
				_ = out.Close()
				return fmt.Errorf("write file %q: %w", target, err)
			}
			if err := out.Close(); err != nil {
				return fmt.Errorf("close file %q: %w", target, err)
			}
		}
	}
}

func extractZip(archivePath, destDir string) error {
	zr, err := zip.OpenReader(archivePath)
	if err != nil {
		return fmt.Errorf("open zip: %w", err)
	}
	defer zr.Close()

	for _, f := range zr.File {
		target, err := safeJoin(destDir, f.Name)
		if err != nil {
			return err
		}

		if f.FileInfo().IsDir() {
			if err := os.MkdirAll(target, 0o755); err != nil {
				return fmt.Errorf("create directory %q: %w", target, err)
			}
			continue
		}

		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			return fmt.Errorf("create parent directory for %q: %w", target, err)
		}

		rc, err := f.Open()
		if err != nil {
			return fmt.Errorf("open zip entry %q: %w", f.Name, err)
		}
		perm := fileModeOrDefault(f.Mode(), 0o644)
		out, err := os.OpenFile(target, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, perm)
		if err != nil {
			_ = rc.Close()
			return fmt.Errorf("create file %q: %w", target, err)
		}
		if _, err := io.Copy(out, rc); err != nil {
			_ = out.Close()
			_ = rc.Close()
			return fmt.Errorf("write file %q: %w", target, err)
		}
		if err := out.Close(); err != nil {
			_ = rc.Close()
			return fmt.Errorf("close file %q: %w", target, err)
		}
		if err := rc.Close(); err != nil {
			return fmt.Errorf("close zip entry %q: %w", f.Name, err)
		}
	}

	return nil
}

func safeJoin(baseDir, entryName string) (string, error) {
	clean := filepath.Clean(entryName)
	if clean == "." || clean == "" {
		return "", errors.New("archive entry path is empty")
	}
	if strings.HasPrefix(clean, "..") || filepath.IsAbs(clean) {
		return "", fmt.Errorf("unsafe archive entry path: %q", entryName)
	}

	baseAbs, err := filepath.Abs(baseDir)
	if err != nil {
		return "", fmt.Errorf("resolve destination directory: %w", err)
	}
	target := filepath.Join(baseAbs, clean)
	targetAbs, err := filepath.Abs(target)
	if err != nil {
		return "", fmt.Errorf("resolve target path: %w", err)
	}
	if targetAbs != baseAbs && !strings.HasPrefix(targetAbs, baseAbs+string(os.PathSeparator)) {
		return "", fmt.Errorf("unsafe archive entry path: %q", entryName)
	}

	return targetAbs, nil
}

func fileModeOrDefault(mode fs.FileMode, def fs.FileMode) fs.FileMode {
	perm := mode.Perm()
	if perm == 0 {
		return def
	}
	return perm
}
