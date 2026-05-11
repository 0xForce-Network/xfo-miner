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

func TestClassifyHashcatUnsupportedPatterns(t *testing.T) {
	tests := []struct {
		name   string
		text   string
		reason string
	}{
		{name: "module", text: "module not found", reason: HashcatUnsupportedReasonModuleMissing},
		{name: "mode", text: "Unknown hash mode 26620", reason: HashcatUnsupportedReasonModeUnsupported},
		{name: "attack", text: "Unsupported attack mode", reason: HashcatUnsupportedReasonAttackUnsupported},
		{name: "token", text: "Token length exception", reason: HashcatUnsupportedReasonTokenShape},
		{name: "format", text: "Separator unmatched", reason: HashcatUnsupportedReasonHashFormat},
		{name: "kernel", text: "OpenCL kernel unavailable", reason: HashcatUnsupportedReasonKernelUnsupported},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reason, ok := classifyHashcatUnsupported(tt.text)
			if !ok {
				t.Fatalf("expected unsupported classification")
			}
			if reason != tt.reason {
				t.Fatalf("expected reason %q, got %q", tt.reason, reason)
			}
		})
	}
}

func TestSanitizeHashcatEvidenceSummaryRedactsSecrets(t *testing.T) {
	summary := sanitizeHashcatEvidenceSummary(strings.Join([]string{
		"Plaintext........: secret-password",
		"$metamask$600000$AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA$BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB$CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC",
		"module not found for mode",
	}, "\n"))
	if strings.Contains(summary, "secret-password") || strings.Contains(summary, "$metamask") {
		t.Fatalf("summary leaked sensitive material: %s", summary)
	}
	if !strings.Contains(summary, "module not found") {
		t.Fatalf("summary should retain safe diagnostic text: %s", summary)
	}
}

func TestHashcatRunnerProbeSupported(t *testing.T) {
	tempDir := t.TempDir()
	fakeHashcatPath := filepath.Join(tempDir, "fake_hashcat_probe_supported.sh")
	script := strings.Join([]string{
		"#!/bin/sh",
		"set -eu",
		"echo 'hashcat accepted'",
	}, "\n") + "\n"
	if err := os.WriteFile(fakeHashcatPath, []byte(script), 0o755); err != nil {
		t.Fatalf("write fake hashcat script: %v", err)
	}

	runner := NewHashcatRunner(process.NewRealManager(discardLogger()), fakeHashcatPath, discardLogger())
	probe := sampleHashcatProbe(5000)
	result, err := runner.Probe(context.Background(), probe)
	if err != nil {
		t.Fatalf("Probe() error = %v", err)
	}
	if result.Status != "supported" || result.ReasonCode != "ok" {
		t.Fatalf("expected supported ok, got %#v", result)
	}
}

func TestHashcatRunnerProbeTreatsCompletedNoMatchAsSupported(t *testing.T) {
	tempDir := t.TempDir()
	fakeHashcatPath := filepath.Join(tempDir, "fake_hashcat_probe_completed_no_match.sh")
	script := strings.Join([]string{
		"#!/bin/sh",
		"set -eu",
		"echo 'Recovered........: 0/1 (0.00%) Digests (total), 0/1 (0.00%) Digests (new)'",
		"echo 'Progress.........: 1/1 (100.00%)'",
		"echo 'Status...........: Exhausted'",
		"exit 1",
	}, "\n") + "\n"
	if err := os.WriteFile(fakeHashcatPath, []byte(script), 0o755); err != nil {
		t.Fatalf("write fake hashcat script: %v", err)
	}

	runner := NewHashcatRunner(process.NewRealManager(discardLogger()), fakeHashcatPath, discardLogger())
	result, err := runner.Probe(context.Background(), sampleHashcatProbe(5000))
	if err != nil {
		t.Fatalf("Probe() error = %v", err)
	}
	if result.Status != "supported" || result.ReasonCode != "ok" {
		t.Fatalf("expected completed no-match probe to be supported ok, got %#v", result)
	}
}

func TestHashcatRunnerProbeUnsupported(t *testing.T) {
	tempDir := t.TempDir()
	fakeHashcatPath := filepath.Join(tempDir, "fake_hashcat_probe_unsupported.sh")
	script := strings.Join([]string{
		"#!/bin/sh",
		"set -eu",
		"echo 'Module not found for requested mode' >&2",
		"exit 1",
	}, "\n") + "\n"
	if err := os.WriteFile(fakeHashcatPath, []byte(script), 0o755); err != nil {
		t.Fatalf("write fake hashcat script: %v", err)
	}

	runner := NewHashcatRunner(process.NewRealManager(discardLogger()), fakeHashcatPath, discardLogger())
	result, err := runner.Probe(context.Background(), sampleHashcatProbe(5000))
	if err != nil {
		t.Fatalf("Probe() error = %v", err)
	}
	if result.Status != "unsupported" {
		t.Fatalf("expected unsupported, got %#v", result)
	}
	if result.ReasonCode != HashcatUnsupportedReasonModuleMissing {
		t.Fatalf("expected module missing, got %q", result.ReasonCode)
	}
}

func TestHashcatRunnerProbeTimeout(t *testing.T) {
	tempDir := t.TempDir()
	fakeHashcatPath := filepath.Join(tempDir, "fake_hashcat_probe_timeout.sh")
	script := strings.Join([]string{
		"#!/bin/sh",
		"set -eu",
		"sleep 2",
	}, "\n") + "\n"
	if err := os.WriteFile(fakeHashcatPath, []byte(script), 0o755); err != nil {
		t.Fatalf("write fake hashcat script: %v", err)
	}

	runner := NewHashcatRunner(process.NewRealManager(discardLogger()), fakeHashcatPath, discardLogger())
	result, err := runner.Probe(context.Background(), sampleHashcatProbe(50))
	if err != nil {
		t.Fatalf("Probe() error = %v", err)
	}
	if result.Status != "error" || result.ReasonCode != "hashcat_probe_timeout" {
		t.Fatalf("expected timeout error, got %#v", result)
	}
}

func TestHashcatRunnerProbeDictionarySliceMaterializesSyntheticDictionary(t *testing.T) {
	tempDir := t.TempDir()
	argsCapturePath := filepath.Join(tempDir, "probe_dictionary_args.txt")
	wordlistCapturePath := filepath.Join(tempDir, "probe_dictionary_wordlist.txt")
	fakeHashcatPath := filepath.Join(tempDir, "fake_hashcat_probe_dictionary_supported.sh")
	script := strings.Join([]string{
		"#!/bin/sh",
		"set -eu",
		"printf '%s\n' \"$@\" > \"" + argsCapturePath + "\"",
		"wordlist=''",
		"for arg in \"$@\"; do",
		"  case \"$arg\" in",
		"    *.wordlist)",
		"      wordlist=\"$arg\"",
		"      ;;",
		"  esac",
		"done",
		"if [ -z \"$wordlist\" ]; then",
		"  echo 'missing synthetic dictionary' >&2",
		"  exit 1",
		"fi",
		"cp \"$wordlist\" \"" + wordlistCapturePath + "\"",
		"echo 'hashcat accepted dictionary probe'",
	}, "\n") + "\n"
	if err := os.WriteFile(fakeHashcatPath, []byte(script), 0o755); err != nil {
		t.Fatalf("write fake hashcat script: %v", err)
	}

	runner := NewHashcatRunner(process.NewRealManager(discardLogger()), fakeHashcatPath, discardLogger())
	result, err := runner.Probe(context.Background(), sampleDictionarySliceProbe(5000))
	if err != nil {
		t.Fatalf("Probe() error = %v", err)
	}
	if result.Status != "supported" || result.ReasonCode != "ok" {
		t.Fatalf("expected supported dictionary probe, got %#v", result)
	}

	argsBytes, err := os.ReadFile(argsCapturePath)
	if err != nil {
		t.Fatalf("read args capture: %v", err)
	}
	argsText := string(argsBytes)
	if !strings.Contains(argsText, "-a\n0\n") {
		t.Fatalf("expected attack mode 0, args:\n%s", argsText)
	}
	if !strings.Contains(argsText, ".wordlist\n") {
		t.Fatalf("expected synthetic wordlist input, args:\n%s", argsText)
	}
	wordlistBytes, err := os.ReadFile(wordlistCapturePath)
	if err != nil {
		t.Fatalf("read wordlist capture: %v", err)
	}
	if strings.TrimSpace(string(wordlistBytes)) != "xfo-probe-candidate" {
		t.Fatalf("unexpected synthetic wordlist content: %q", strings.TrimSpace(string(wordlistBytes)))
	}
}

func TestHashcatRunnerProbeDictionarySliceUnsupportedFromFakeHashcat(t *testing.T) {
	tempDir := t.TempDir()
	fakeHashcatPath := filepath.Join(tempDir, "fake_hashcat_probe_dictionary_unsupported.sh")
	script := strings.Join([]string{
		"#!/bin/sh",
		"set -eu",
		"echo 'Unsupported attack mode' >&2",
		"exit 1",
	}, "\n") + "\n"
	if err := os.WriteFile(fakeHashcatPath, []byte(script), 0o755); err != nil {
		t.Fatalf("write fake hashcat script: %v", err)
	}

	runner := NewHashcatRunner(process.NewRealManager(discardLogger()), fakeHashcatPath, discardLogger())
	result, err := runner.Probe(context.Background(), sampleDictionarySliceProbe(5000))
	if err != nil {
		t.Fatalf("Probe() error = %v", err)
	}
	if result.Status != "unsupported" {
		t.Fatalf("expected unsupported dictionary probe, got %#v", result)
	}
	if result.ReasonCode != HashcatUnsupportedReasonAttackUnsupported {
		t.Fatalf("expected attack unsupported, got %q", result.ReasonCode)
	}
}

func TestSchedulerRejectsHashcatProbeArgs(t *testing.T) {
	s, _, pcl, _ := newTestScheduler()
	probe := sampleHashcatProbe(5000)
	probe.ProbePayload.Args = []string{"--outfile", "/tmp/unsafe"}
	s.handleMessage(context.Background(), inboundMessage{msgType: "hashcat_capability_probe", raw: mustMarshalProbe(t, probe)})
	result := pcl.latestHashcatProbeResult()
	if result == nil {
		t.Fatalf("expected rejected hashcat probe result")
	}
	if result.Status != "error" || result.ReasonCode != "invalid_probe_contract" {
		t.Fatalf("expected invalid contract error, got %#v", result)
	}
	if !strings.Contains(result.ErrorSummary, "probe_payload.args") {
		t.Fatalf("expected args rejection summary, got %q", result.ErrorSummary)
	}
}

func TestHashcatRunnerRuntimeUnsupportedResult(t *testing.T) {
	tempDir := t.TempDir()
	fakeHashcatPath := filepath.Join(tempDir, "fake_hashcat_runtime_unsupported.sh")
	script := strings.Join([]string{
		"#!/bin/sh",
		"set -eu",
		"echo 'Unknown hash mode 26620' >&2",
		"echo 'Plaintext........: should-not-leak' >&2",
		"exit 1",
	}, "\n") + "\n"
	if err := os.WriteFile(fakeHashcatPath, []byte(script), 0o755); err != nil {
		t.Fatalf("write fake hashcat script: %v", err)
	}
	targetPath := filepath.Join(tempDir, "sample.hash")
	if err := os.WriteFile(targetPath, []byte("dummy-target"), 0o600); err != nil {
		t.Fatalf("write target file: %v", err)
	}

	runner := NewHashcatRunner(process.NewRealManager(discardLogger()), fakeHashcatPath, discardLogger())
	job := &pool.JobGPUMessage{Type: "job_gpu", JobID: "runtime-unsupported", CapabilityFingerprint: "sha256:abc", HashMode: 26620, Target: targetPath, Skip: 0, Limit: 1}
	var result *pool.ResultMessage
	if err := runner.Run(context.Background(), job, nil, func(msg *pool.ResultMessage) { copy := *msg; result = &copy }); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if result == nil {
		t.Fatalf("expected result callback")
	}
	if result.Status != "hashcat_unsupported" {
		t.Fatalf("expected hashcat_unsupported, got %#v", result)
	}
	var payload pool.HashcatUnsupportedData
	if err := json.Unmarshal([]byte(result.Data), &payload); err != nil {
		t.Fatalf("unmarshal unsupported payload: %v", err)
	}
	if payload.CapabilityFingerprint != "sha256:abc" {
		t.Fatalf("expected fingerprint sha256:abc, got %q", payload.CapabilityFingerprint)
	}
	if payload.ReasonCode != HashcatUnsupportedReasonModeUnsupported {
		t.Fatalf("expected mode unsupported, got %q", payload.ReasonCode)
	}
	if strings.Contains(payload.ErrorSummary, "should-not-leak") {
		t.Fatalf("runtime summary leaked plaintext: %s", payload.ErrorSummary)
	}
}

func TestSchedulerHandlesHashcatCapabilityProbe(t *testing.T) {
	s, _, pcl, _ := newTestScheduler()
	s.capabilities.HashcatVersion = "v7.1.2-xfo"
	s.handleMessage(context.Background(), inboundMessage{msgType: "hashcat_capability_probe", raw: mustMarshalProbe(t, sampleHashcatProbe(5000))})
	result := pcl.latestHashcatProbeResult()
	if result == nil {
		t.Fatalf("expected hashcat probe result")
	}
	if result.Type != "hashcat_capability_probe_result" {
		t.Fatalf("unexpected type %q", result.Type)
	}
	if result.Status != "supported" || result.ReasonCode != "ok" {
		t.Fatalf("expected supported ok, got %#v", result)
	}
	if result.ProbeID != "probe-test" || result.CapabilityFingerprint != "sha256:abc" {
		t.Fatalf("probe identity mismatch: %#v", result)
	}
}

func TestSchedulerRejectsInvalidHashcatCapabilityProbe(t *testing.T) {
	s, _, pcl, _ := newTestScheduler()
	probe := sampleHashcatProbe(5000)
	probe.ProbeID = ""
	s.handleMessage(context.Background(), inboundMessage{msgType: "hashcat_capability_probe", raw: mustMarshalProbe(t, probe)})
	result := pcl.latestHashcatProbeResult()
	if result == nil {
		t.Fatalf("expected rejected hashcat probe result")
	}
	if result.Status != "error" || result.ReasonCode != "invalid_probe_contract" {
		t.Fatalf("expected invalid contract error, got %#v", result)
	}
}

func sampleHashcatProbe(timeoutMS int) *pool.HashcatCapabilityProbeMessage {
	return &pool.HashcatCapabilityProbeMessage{
		Type:                  "hashcat_capability_probe",
		ProbeID:               "probe-test",
		CapabilityFingerprint: "sha256:abc",
		JobShape: pool.HashcatProbeJobShape{
			HashMode: 22000,
			KeyspaceContract: json.RawMessage(`{
				"type": "fixed_candidate_list",
				"candidates": ["password"]
			}`),
		},
		ProbePayload: pool.HashcatProbePayload{TargetSample: "sample-hash"},
		TimeoutMS:    timeoutMS,
	}
}

func sampleDictionarySliceProbe(timeoutMS int) *pool.HashcatCapabilityProbeMessage {
	attackMode := 0
	return &pool.HashcatCapabilityProbeMessage{
		Type:                  "hashcat_capability_probe",
		ProbeID:               "probe-dictionary-test",
		CapabilityFingerprint: "sha256:dict",
		JobShape: pool.HashcatProbeJobShape{
			HashMode:   22000,
			AttackMode: &attackMode,
			Dictionary: &pool.DictionarySpec{
				DictID:         "bt2024",
				CompressFormat: "none",
			},
			KeyspaceContract: json.RawMessage(`{"type":"dictionary_slice"}`),
		},
		ProbePayload: pool.HashcatProbePayload{TargetSample: "sample-hash"},
		TimeoutMS:    timeoutMS,
	}
}

func mustMarshalProbe(t *testing.T, probe *pool.HashcatCapabilityProbeMessage) json.RawMessage {
	t.Helper()
	b, err := json.Marshal(probe)
	if err != nil {
		t.Fatalf("marshal probe: %v", err)
	}
	return b
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}
