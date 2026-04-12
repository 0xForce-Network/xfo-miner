package scheduler

import (
	"errors"
	"os"
	"testing"
)

func TestMaterializeKeyspaceContractWithoutContractKeepsFallbackRange(t *testing.T) {
	materialized, err := materializeKeyspaceContract("job-plain", nil, 12, 34)
	if err != nil {
		t.Fatalf("materializeKeyspaceContract() error = %v", err)
	}
	if materialized.AttackMode != nil {
		t.Fatalf("expected nil attack mode without keyspace contract")
	}
	if materialized.Skip != 12 || materialized.Limit != 34 {
		t.Fatalf("unexpected fallback range: skip=%d limit=%d", materialized.Skip, materialized.Limit)
	}
	if len(materialized.Inputs) != 0 {
		t.Fatalf("expected no extra hashcat inputs, got %v", materialized.Inputs)
	}
}

func TestMaterializeKeyspaceContractFixedCandidateList(t *testing.T) {
	raw := []byte(`{"type":"fixed_candidate_list","candidates":["000000000000","123123456456","999999999999"]}`)
	materialized, err := materializeKeyspaceContract("job-fixed", raw, 0, 3)
	if err != nil {
		t.Fatalf("materializeKeyspaceContract() error = %v", err)
	}
	if materialized.AttackMode == nil || *materialized.AttackMode != 0 {
		t.Fatalf("expected attack mode 0 for fixed_candidate_list")
	}
	if len(materialized.Inputs) != 1 {
		t.Fatalf("expected one wordlist input, got %v", materialized.Inputs)
	}
	if len(materialized.CleanupPaths) != 1 {
		t.Fatalf("expected one cleanup path, got %v", materialized.CleanupPaths)
	}

	wordlistPath := materialized.Inputs[0]
	defer os.Remove(wordlistPath)

	content, err := os.ReadFile(wordlistPath)
	if err != nil {
		t.Fatalf("read generated wordlist: %v", err)
	}
	expected := "000000000000\n123123456456\n999999999999\n"
	if string(content) != expected {
		t.Fatalf("unexpected wordlist content:\nwant: %q\n got: %q", expected, string(content))
	}
}

func TestMaterializeKeyspaceContractFixedCandidateListOverridesFallbackRange(t *testing.T) {
	raw := []byte(`{"type":"fixed_candidate_list","candidates":["abc","def","ghi"]}`)
	materialized, err := materializeKeyspaceContract("job-fixed-override", raw, 100, 0)
	if err != nil {
		t.Fatalf("materializeKeyspaceContract() error = %v", err)
	}

	if materialized.Skip != 0 {
		t.Fatalf("expected fixed_candidate_list skip override to 0, got %d", materialized.Skip)
	}
	if materialized.Limit != 3 {
		t.Fatalf("expected fixed_candidate_list limit override to candidate count (3), got %d", materialized.Limit)
	}

	for _, cleanupPath := range materialized.CleanupPaths {
		_ = os.Remove(cleanupPath)
	}
}

func TestMaterializeKeyspaceContractMaskSegmentOverrideRange(t *testing.T) {
	raw := []byte(`{"type":"mask_segment","mask":"?d?d?d?d","skip":5,"limit":12}`)
	materialized, err := materializeKeyspaceContract("job-mask", raw, 1, 2)
	if err != nil {
		t.Fatalf("materializeKeyspaceContract() error = %v", err)
	}
	if materialized.AttackMode == nil || *materialized.AttackMode != 3 {
		t.Fatalf("expected attack mode 3 for mask_segment")
	}
	if len(materialized.Inputs) != 1 || materialized.Inputs[0] != "?d?d?d?d" {
		t.Fatalf("unexpected mask input: %v", materialized.Inputs)
	}
	if materialized.Skip != 5 || materialized.Limit != 12 {
		t.Fatalf("unexpected mask range override: skip=%d limit=%d", materialized.Skip, materialized.Limit)
	}
}

func TestMaterializeKeyspaceContractRejectsInvalidFixedCandidateList(t *testing.T) {
	raw := []byte(`{"type":"fixed_candidate_list","candidates":["", "ok"]}`)
	_, err := materializeKeyspaceContract("job-invalid", raw, 0, 1)
	if !errors.Is(err, ErrInvalidKeyspaceContract) {
		t.Fatalf("expected ErrInvalidKeyspaceContract, got %v", err)
	}
}

func TestMaterializeKeyspaceContractRejectsUnsupportedTypes(t *testing.T) {
	tests := []string{
		`{"type":"dictionary_slice","dictionary_url":"https://pool/artifacts/dict.txt"}`,
		`{"type":"deterministic_range","start":1,"end":32}`,
	}

	for _, raw := range tests {
		_, err := materializeKeyspaceContract("job-unsupported", []byte(raw), 0, 1)
		if !errors.Is(err, ErrUnsupportedKeyspaceContract) {
			t.Fatalf("expected ErrUnsupportedKeyspaceContract for raw=%s, got %v", raw, err)
		}
	}
}
