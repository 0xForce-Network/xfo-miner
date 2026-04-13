package scheduler

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/0xforce/xfo-miner/internal/pool"
	"github.com/0xforce/xfo-miner/internal/process"
)

func TestHashcatRunnerFixedCandidateList504HappyPathEvidence(t *testing.T) {
	tempDir := t.TempDir()
	argsCapturePath := filepath.Join(tempDir, "captured_args.txt")
	wordlistCapturePath := filepath.Join(tempDir, "captured_wordlist.txt")

	fakeHashcatPath := filepath.Join(tempDir, "fake_hashcat.sh")
	script := strings.Join([]string{
		"#!/bin/sh",
		"set -eu",
		"printf '%s\\n' \"$@\" > \"" + argsCapturePath + "\"",
		"wordlist=''",
		"for arg in \"$@\"; do",
		"  case \"$arg\" in",
		"    *.wordlist)",
		"      wordlist=\"$arg\"",
		"      ;;",
		"  esac",
		"done",
		"if [ -n \"$wordlist\" ]; then",
		"  cp \"$wordlist\" \"" + wordlistCapturePath + "\"",
		"fi",
		"printf 'Status...........: Cracked\\n'",
		"printf 'Plaintext........: 123123456456\\n'",
	}, "\n") + "\n"
	if err := os.WriteFile(fakeHashcatPath, []byte(script), 0o755); err != nil {
		t.Fatalf("write fake hashcat script: %v", err)
	}

	targetPath := filepath.Join(tempDir, "sample_504.hc22000")
	if err := os.WriteFile(targetPath, []byte("dummy-504-target"), 0o600); err != nil {
		t.Fatalf("write target file: %v", err)
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	runner := NewHashcatRunner(process.NewRealManager(logger), fakeHashcatPath, logger)

	job := &pool.JobGPUMessage{
		Type:     "job_gpu",
		JobID:    "job-504-evidence",
		HashMode: 22000,
		Target:   targetPath,
		Skip:     0,
		Limit:    3,
		KeyspaceContract: json.RawMessage(`{
			"type": "fixed_candidate_list",
			"candidates": ["000000000000", "123123456456", "999999999999"]
		}`),
	}

	var result *pool.ResultMessage
	err := runner.Run(context.Background(), job, nil, func(msg *pool.ResultMessage) {
		copy := *msg
		result = &copy
	})
	if err != nil {
		t.Fatalf("HashcatRunner.Run() error = %v", err)
	}

	if result == nil {
		t.Fatalf("expected result callback")
	}
	if result.Status != "cracked" {
		t.Fatalf("expected status cracked, got %q", result.Status)
	}
	if result.Data != "123123456456" {
		t.Fatalf("expected plaintext 123123456456, got %q", result.Data)
	}

	argsBytes, err := os.ReadFile(argsCapturePath)
	if err != nil {
		t.Fatalf("read args capture: %v", err)
	}
	argsText := string(argsBytes)
	if !strings.Contains(argsText, "-a\n0\n") {
		t.Fatalf("expected hashcat args to include attack mode 0, args:\n%s", argsText)
	}
	if !strings.Contains(argsText, targetPath+"\n") {
		t.Fatalf("expected hashcat args to include target path, args:\n%s", argsText)
	}

	wordlistBytes, err := os.ReadFile(wordlistCapturePath)
	if err != nil {
		t.Fatalf("read captured wordlist: %v", err)
	}
	wordlistText := string(wordlistBytes)
	if !strings.Contains(wordlistText, "123123456456\n") {
		t.Fatalf("expected candidate list to include 123123456456, wordlist:\n%s", wordlistText)
	}
}

func TestHashcatRunnerFixedCandidateListRecoversPlaintextFromOutfile(t *testing.T) {
	tempDir := t.TempDir()
	outfileCapturePath := filepath.Join(tempDir, "captured_outfile.txt")

	fakeHashcatPath := filepath.Join(tempDir, "fake_hashcat_outfile.sh")
	script := strings.Join([]string{
		"#!/bin/sh",
		"set -eu",
		"outfile=''",
		"while [ $# -gt 0 ]; do",
		"  case \"$1\" in",
		"    --outfile)",
		"      outfile=\"$2\"",
		"      shift 2",
		"      ;;",
		"    *)",
		"      shift",
		"      ;;",
		"  esac",
		"done",
		"if [ -z \"$outfile\" ]; then",
		"  echo 'missing outfile' >&2",
		"  exit 1",
		"fi",
		"printf '123123456456\n' > \"$outfile\"",
		"cp \"$outfile\" \"" + outfileCapturePath + "\"",
	}, "\n") + "\n"
	if err := os.WriteFile(fakeHashcatPath, []byte(script), 0o755); err != nil {
		t.Fatalf("write fake hashcat script: %v", err)
	}

	targetPath := filepath.Join(tempDir, "sample_504.hc22000")
	if err := os.WriteFile(targetPath, []byte("dummy-504-target"), 0o600); err != nil {
		t.Fatalf("write target file: %v", err)
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	runner := NewHashcatRunner(process.NewRealManager(logger), fakeHashcatPath, logger)

	job := &pool.JobGPUMessage{
		Type:     "job_gpu",
		JobID:    "job-504-outfile",
		HashMode: 22000,
		Target:   targetPath,
		Skip:     0,
		Limit:    3,
		KeyspaceContract: json.RawMessage(`{
			"type": "fixed_candidate_list",
			"candidates": ["000000000000", "123123456456", "999999999999"]
		}`),
	}

	var result *pool.ResultMessage
	err := runner.Run(context.Background(), job, nil, func(msg *pool.ResultMessage) {
		copy := *msg
		result = &copy
	})
	if err != nil {
		t.Fatalf("HashcatRunner.Run() error = %v", err)
	}

	if result == nil {
		t.Fatalf("expected result callback")
	}
	if result.Status != "cracked" {
		t.Fatalf("expected status cracked, got %q", result.Status)
	}
	if result.Data != "123123456456" {
		t.Fatalf("expected plaintext 123123456456, got %q", result.Data)
	}

	outfileBytes, err := os.ReadFile(outfileCapturePath)
	if err != nil {
		t.Fatalf("read outfile capture: %v", err)
	}
	if strings.TrimSpace(string(outfileBytes)) != "123123456456" {
		t.Fatalf("expected outfile plaintext 123123456456, got %q", strings.TrimSpace(string(outfileBytes)))
	}
}

func TestHashcatRunnerDictionarySliceUsesRuntimePathAndRangeArgs(t *testing.T) {
	tempDir := t.TempDir()
	argsCapturePath := filepath.Join(tempDir, "captured_dictionary_slice_args.txt")

	fakeHashcatPath := filepath.Join(tempDir, "fake_hashcat_dictionary_slice.sh")
	script := strings.Join([]string{
		"#!/bin/sh",
		"set -eu",
		"printf '%s\\n' \"$@\" > \"" + argsCapturePath + "\"",
		"printf 'Status...........: Cracked\\n'",
		"printf 'Plaintext........: 123123456456\\n'",
	}, "\n") + "\n"
	if err := os.WriteFile(fakeHashcatPath, []byte(script), 0o755); err != nil {
		t.Fatalf("write fake hashcat script: %v", err)
	}

	targetPath := filepath.Join(tempDir, "sample_dictionary_slice.hc22000")
	if err := os.WriteFile(targetPath, []byte("dummy-dictionary-target"), 0o600); err != nil {
		t.Fatalf("write target file: %v", err)
	}
	dictionaryPath := filepath.Join(tempDir, "bt2024.txt")
	if err := os.WriteFile(dictionaryPath, []byte("foo\nbar\n"), 0o600); err != nil {
		t.Fatalf("write dictionary file: %v", err)
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	runner := NewHashcatRunner(process.NewRealManager(logger), fakeHashcatPath, logger)

	job := &pool.JobGPUMessage{
		Type:     "job_gpu",
		JobID:    "job-dictionary-slice-evidence",
		HashMode: 22000,
		Target:   targetPath,
		Skip:     11,
		Limit:    22,
		Dictionary: &pool.DictionarySpec{
			RuntimePath: dictionaryPath,
		},
		KeyspaceContract: json.RawMessage(`{"type":"dictionary_slice"}`),
	}

	var result *pool.ResultMessage
	err := runner.Run(context.Background(), job, nil, func(msg *pool.ResultMessage) {
		copy := *msg
		result = &copy
	})
	if err != nil {
		t.Fatalf("HashcatRunner.Run() error = %v", err)
	}
	if result == nil {
		t.Fatalf("expected result callback")
	}
	if result.Status != "cracked" {
		t.Fatalf("expected status cracked, got %q", result.Status)
	}

	argsBytes, err := os.ReadFile(argsCapturePath)
	if err != nil {
		t.Fatalf("read args capture: %v", err)
	}
	argsText := string(argsBytes)
	if !strings.Contains(argsText, "-a\n0\n") {
		t.Fatalf("expected hashcat args to include attack mode 0, args:\n%s", argsText)
	}
	if !strings.Contains(argsText, targetPath+"\n") {
		t.Fatalf("expected hashcat args to include target path, args:\n%s", argsText)
	}
	if !strings.Contains(argsText, dictionaryPath+"\n") {
		t.Fatalf("expected hashcat args to include dictionary runtime path, args:\n%s", argsText)
	}
	if !strings.Contains(argsText, "--skip\n11\n") {
		t.Fatalf("expected hashcat args to include --skip 11, args:\n%s", argsText)
	}
	if !strings.Contains(argsText, "--limit\n22\n") {
		t.Fatalf("expected hashcat args to include --limit 22, args:\n%s", argsText)
	}
}
