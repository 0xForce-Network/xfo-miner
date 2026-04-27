package updater

import (
	"archive/tar"
	"archive/zip"
	"bytes"
	"compress/gzip"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func TestExtractArchiveTarGz(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	archivePath := filepath.Join(tmpDir, "pkg.tar.gz")
	destDir := filepath.Join(tmpDir, "out")

	buf := bytes.NewBuffer(nil)
	gzw := gzip.NewWriter(buf)
	tw := tar.NewWriter(gzw)
	payload := []byte("binary-data")
	h := &tar.Header{Name: "release/xfo-miner", Mode: 0o755, Size: int64(len(payload))}
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
	if err := os.WriteFile(archivePath, buf.Bytes(), 0o644); err != nil {
		t.Fatalf("write archive file: %v", err)
	}

	if err := ExtractArchive(archivePath, destDir); err != nil {
		t.Fatalf("ExtractArchive() error = %v", err)
	}

	extracted := filepath.Join(destDir, "release", "xfo-miner")
	got, err := os.ReadFile(extracted)
	if err != nil {
		t.Fatalf("read extracted file: %v", err)
	}
	if string(got) != string(payload) {
		t.Fatalf("unexpected extracted payload: got %q want %q", string(got), string(payload))
	}
}

func TestExtractArchiveZip(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	archivePath := filepath.Join(tmpDir, "pkg.zip")
	destDir := filepath.Join(tmpDir, "out")

	buf := bytes.NewBuffer(nil)
	zw := zip.NewWriter(buf)
	fw, err := zw.Create("release/xfo-miner.exe")
	if err != nil {
		t.Fatalf("create zip entry: %v", err)
	}
	payload := []byte("windows-binary")
	if _, err := fw.Write(payload); err != nil {
		t.Fatalf("write zip payload: %v", err)
	}
	if err := zw.Close(); err != nil {
		t.Fatalf("close zip writer: %v", err)
	}
	if err := os.WriteFile(archivePath, buf.Bytes(), 0o644); err != nil {
		t.Fatalf("write zip archive: %v", err)
	}

	if err := ExtractArchive(archivePath, destDir); err != nil {
		t.Fatalf("ExtractArchive() error = %v", err)
	}

	extracted := filepath.Join(destDir, "release", "xfo-miner.exe")
	got, err := os.ReadFile(extracted)
	if err != nil {
		t.Fatalf("read extracted file: %v", err)
	}
	if string(got) != string(payload) {
		t.Fatalf("unexpected extracted payload: got %q want %q", string(got), string(payload))
	}
}

func TestExtractArchiveInvalid(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	archivePath := filepath.Join(tmpDir, "pkg.bin")
	destDir := filepath.Join(tmpDir, "out")
	if err := os.WriteFile(archivePath, []byte("not-an-archive"), 0o644); err != nil {
		t.Fatalf("write invalid artifact: %v", err)
	}

	if err := ExtractArchive(archivePath, destDir); err == nil {
		t.Fatalf("expected ExtractArchive() to fail for invalid archive")
	}
}

func TestLocateExtractedBinaryFindsPlatformPackagedName(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	releaseDir := filepath.Join(tmpDir, "release")
	if err := os.MkdirAll(releaseDir, 0o755); err != nil {
		t.Fatalf("mkdir release dir: %v", err)
	}

	binaryName := packagedBinaryName(runtime.GOOS, runtime.GOARCH)
	binaryPath := filepath.Join(releaseDir, binaryName)
	if err := os.WriteFile(binaryPath, []byte("binary-data"), 0o755); err != nil {
		t.Fatalf("write packaged binary: %v", err)
	}

	found, err := locateExtractedBinary(tmpDir, "xfo-miner")
	if err != nil {
		t.Fatalf("locateExtractedBinary() error = %v", err)
	}
	if found != binaryPath {
		t.Fatalf("unexpected located binary: got %q want %q", found, binaryPath)
	}
}
