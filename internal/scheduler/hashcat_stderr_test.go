package scheduler

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/0xforce/xfo-miner/internal/pool"
	"github.com/0xforce/xfo-miner/internal/process"
)

func TestHashcatRunnerStderrDoesNotBlockResult(t *testing.T) {
	tempDir := t.TempDir()
	fakeHashcatPath := filepath.Join(tempDir, "fake_hashcat_stderr_stdout.sh")
	script := strings.Join([]string{
		"#!/bin/sh",
		"set -eu",
		"echo 'OpenCL Platform #1' >&2",
		"echo 'Device #1: GPU' >&2",
		"echo 'Temperature warning' >&2",
		"echo 'Status...........: Cracked'",
		"echo 'Plaintext........: testpassword'",
	}, "\n") + "\n"
	if err := os.WriteFile(fakeHashcatPath, []byte(script), 0o755); err != nil {
		t.Fatalf("write fake hashcat script: %v", err)
	}

	targetPath := filepath.Join(tempDir, "sample.hc22000")
	if err := os.WriteFile(targetPath, []byte("dummy-target"), 0o600); err != nil {
		t.Fatalf("write target file: %v", err)
	}

	runner := NewHashcatRunner(process.NewRealManager(slog.New(slog.NewTextHandler(io.Discard, nil))), fakeHashcatPath, slog.New(slog.NewTextHandler(io.Discard, nil)))
	job := &pool.JobGPUMessage{Type: "job_gpu", JobID: "stderr-mixed", HashMode: 22000, Target: targetPath, Skip: 0, Limit: 1}

	resultCh := make(chan *pool.ResultMessage, 1)
	errCh := make(chan error, 1)
	go func() {
		errCh <- runner.Run(context.Background(), job, nil, func(msg *pool.ResultMessage) {
			copy := *msg
			resultCh <- &copy
		})
	}()

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("HashcatRunner.Run() error = %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("HashcatRunner.Run() timed out")
	}

	select {
	case result := <-resultCh:
		if result.Status != "cracked" {
			t.Fatalf("expected status cracked, got %q", result.Status)
		}
		if result.Data != "testpassword" {
			t.Fatalf("expected plaintext testpassword, got %q", result.Data)
		}
	default:
		t.Fatal("expected result callback to be called")
	}
}

func TestHashcatRunnerHeavyStderrDoesNotBlock(t *testing.T) {
	tempDir := t.TempDir()
	fakeHashcatPath := filepath.Join(tempDir, "fake_hashcat_heavy_stderr.sh")
	script := strings.Join([]string{
		"#!/bin/sh",
		"set -eu",
		"i=0",
		"while [ \"$i\" -lt 1200 ]; do",
		"  echo \"stderr line $i\" >&2",
		"  i=$((i + 1))",
		"done",
		"echo 'Status...........: Cracked'",
		"echo 'Plaintext........: heavy-password'",
	}, "\n") + "\n"
	if err := os.WriteFile(fakeHashcatPath, []byte(script), 0o755); err != nil {
		t.Fatalf("write fake hashcat script: %v", err)
	}

	targetPath := filepath.Join(tempDir, "sample.hc22000")
	if err := os.WriteFile(targetPath, []byte("dummy-target"), 0o600); err != nil {
		t.Fatalf("write target file: %v", err)
	}

	runner := NewHashcatRunner(process.NewRealManager(slog.New(slog.NewTextHandler(io.Discard, nil))), fakeHashcatPath, slog.New(slog.NewTextHandler(io.Discard, nil)))
	job := &pool.JobGPUMessage{Type: "job_gpu", JobID: "stderr-heavy", HashMode: 22000, Target: targetPath, Skip: 0, Limit: 1}

	resultCh := make(chan *pool.ResultMessage, 1)
	errCh := make(chan error, 1)
	go func() {
		errCh <- runner.Run(context.Background(), job, nil, func(msg *pool.ResultMessage) {
			copy := *msg
			resultCh <- &copy
		})
	}()

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("HashcatRunner.Run() error = %v", err)
		}
	case <-time.After(4 * time.Second):
		t.Fatal("HashcatRunner.Run() timed out with heavy stderr")
	}

	select {
	case result := <-resultCh:
		if result.Status != "cracked" {
			t.Fatalf("expected status cracked, got %q", result.Status)
		}
		if result.Data != "heavy-password" {
			t.Fatalf("expected plaintext heavy-password, got %q", result.Data)
		}
	default:
		t.Fatal("expected result callback to be called")
	}
}

func TestHashcatRunnerStderrOnlyNoStdout(t *testing.T) {
	tempDir := t.TempDir()
	fakeHashcatPath := filepath.Join(tempDir, "fake_hashcat_stderr_only.sh")
	script := strings.Join([]string{
		"#!/bin/sh",
		"set -eu",
		"i=0",
		"while [ \"$i\" -lt 32 ]; do",
		"  echo \"stderr only line $i\" >&2",
		"  i=$((i + 1))",
		"done",
	}, "\n") + "\n"
	if err := os.WriteFile(fakeHashcatPath, []byte(script), 0o755); err != nil {
		t.Fatalf("write fake hashcat script: %v", err)
	}

	targetPath := filepath.Join(tempDir, "sample.hc22000")
	if err := os.WriteFile(targetPath, []byte("dummy-target"), 0o600); err != nil {
		t.Fatalf("write target file: %v", err)
	}

	runner := NewHashcatRunner(process.NewRealManager(slog.New(slog.NewTextHandler(io.Discard, nil))), fakeHashcatPath, slog.New(slog.NewTextHandler(io.Discard, nil)))
	job := &pool.JobGPUMessage{Type: "job_gpu", JobID: "stderr-only", HashMode: 22000, Target: targetPath, Skip: 0, Limit: 1}

	resultCh := make(chan *pool.ResultMessage, 1)
	errCh := make(chan error, 1)
	go func() {
		errCh <- runner.Run(context.Background(), job, nil, func(msg *pool.ResultMessage) {
			copy := *msg
			resultCh <- &copy
		})
	}()

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("HashcatRunner.Run() error = %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("HashcatRunner.Run() timed out with stderr-only output")
	}

	select {
	case result := <-resultCh:
		if result.Status != "exhausted" {
			t.Fatalf("expected status exhausted, got %q", result.Status)
		}
		if result.Data != "" {
			t.Fatalf("expected empty plaintext for stderr-only case, got %q", result.Data)
		}
	default:
		t.Fatal("expected result callback to be called")
	}
}