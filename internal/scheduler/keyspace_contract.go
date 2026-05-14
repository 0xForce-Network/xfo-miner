package scheduler

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/0xforce/xfo-miner/internal/pool"
)

var (
	ErrUnsupportedKeyspaceContract    = errors.New("unsupported_keyspace_contract")
	ErrInvalidKeyspaceContract        = errors.New("invalid_keyspace_contract")
	ErrCandidateMaterializationFailed = errors.New("candidate_materialization_failed")
)

var keyspaceJobIDSanitizer = regexp.MustCompile(`[^a-zA-Z0-9._-]+`)

type keyspaceContract struct {
	Type           string   `json:"type"`
	Candidates     []string `json:"candidates,omitempty"`
	Mask           string   `json:"mask,omitempty"`
	Charset        string   `json:"charset,omitempty"`
	CustomCharset1 string   `json:"custom_charset_1,omitempty"`
	CustomCharset2 string   `json:"custom_charset_2,omitempty"`
	CustomCharset3 string   `json:"custom_charset_3,omitempty"`
	CustomCharset4 string   `json:"custom_charset_4,omitempty"`
	Skip           *int64   `json:"skip,omitempty"`
	Limit          *int64   `json:"limit,omitempty"`
}

type materializedKeyspace struct {
	AttackMode   *int
	Options      []string
	Inputs       []string
	Skip         int64
	Limit        int64
	CleanupPaths []string
}

func materializeKeyspaceContract(jobID string, dictionary *pool.DictionarySpec, raw json.RawMessage, fallbackSkip int64, fallbackLimit int64) (*materializedKeyspace, error) {
	keyspace := &materializedKeyspace{Skip: fallbackSkip, Limit: fallbackLimit}
	if len(raw) == 0 {
		return keyspace, nil
	}

	var contract keyspaceContract
	if err := json.Unmarshal(raw, &contract); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidKeyspaceContract, err)
	}

	contractType := strings.TrimSpace(contract.Type)
	if contractType == "" {
		return nil, fmt.Errorf("%w: missing type", ErrInvalidKeyspaceContract)
	}

	switch contractType {
	case "fixed_candidate_list":
		return materializeFixedCandidateList(jobID, keyspace, contract.Candidates)
	case "mask_segment":
		return materializeMaskSegment(keyspace, contract)
	case "dictionary_slice":
		return materializeDictionarySlice(keyspace, dictionary, contract)
	case "deterministic_range":
		return nil, fmt.Errorf("%w: %s", ErrUnsupportedKeyspaceContract, contractType)
	default:
		return nil, fmt.Errorf("%w: %s", ErrUnsupportedKeyspaceContract, contractType)
	}
}

func materializeDictionarySlice(keyspace *materializedKeyspace, dictionary *pool.DictionarySpec, contract keyspaceContract) (*materializedKeyspace, error) {
	if dictionary == nil {
		return nil, fmt.Errorf("%w: dictionary_slice requires dictionary payload", ErrInvalidKeyspaceContract)
	}

	runtimePath := strings.TrimSpace(dictionary.RuntimePath)
	if runtimePath == "" {
		return nil, fmt.Errorf("%w: dictionary runtime path missing", ErrCandidateMaterializationFailed)
	}

	if contract.Skip != nil {
		if *contract.Skip < 0 {
			return nil, fmt.Errorf("%w: dictionary_slice skip must be non-negative", ErrInvalidKeyspaceContract)
		}
		keyspace.Skip = *contract.Skip
	}
	if contract.Limit != nil {
		if *contract.Limit <= 0 {
			return nil, fmt.Errorf("%w: dictionary_slice limit must be positive", ErrInvalidKeyspaceContract)
		}
		keyspace.Limit = *contract.Limit
	}

	if keyspace.Skip < 0 {
		return nil, fmt.Errorf("%w: dictionary_slice fallback skip must be non-negative", ErrInvalidKeyspaceContract)
	}
	if keyspace.Limit <= 0 {
		return nil, fmt.Errorf("%w: dictionary_slice fallback limit must be positive", ErrInvalidKeyspaceContract)
	}

	attackMode := 0
	keyspace.AttackMode = &attackMode
	keyspace.Inputs = append(keyspace.Inputs, runtimePath)
	return keyspace, nil
}

func materializeFixedCandidateList(jobID string, keyspace *materializedKeyspace, candidates []string) (*materializedKeyspace, error) {
	if len(candidates) == 0 {
		return nil, fmt.Errorf("%w: fixed_candidate_list requires candidates", ErrInvalidKeyspaceContract)
	}

	trimmed := make([]string, 0, len(candidates))
	for _, candidate := range candidates {
		value := strings.TrimSpace(candidate)
		if value == "" {
			return nil, fmt.Errorf("%w: candidate must be non-empty", ErrInvalidKeyspaceContract)
		}
		trimmed = append(trimmed, value)
	}

	wordlistPath, err := writeCandidateWordlist(jobID, trimmed)
	if err != nil {
		return nil, err
	}

	attackMode := 0
	keyspace.AttackMode = &attackMode
	keyspace.Inputs = append(keyspace.Inputs, wordlistPath)
	keyspace.CleanupPaths = append(keyspace.CleanupPaths, wordlistPath)
	keyspace.Skip = 0
	keyspace.Limit = int64(len(trimmed))
	return keyspace, nil
}

func materializeMaskSegment(keyspace *materializedKeyspace, contract keyspaceContract) (*materializedKeyspace, error) {
	mask := strings.TrimSpace(contract.Mask)
	if mask == "" {
		return nil, fmt.Errorf("%w: mask_segment requires mask", ErrInvalidKeyspaceContract)
	}
	customCharsets := []struct {
		flag  string
		value string
		alias string
	}{
		{flag: "-1", value: contract.CustomCharset1, alias: contract.Charset},
		{flag: "-2", value: contract.CustomCharset2},
		{flag: "-3", value: contract.CustomCharset3},
		{flag: "-4", value: contract.CustomCharset4},
	}
	for idx, customCharset := range customCharsets {
		value := strings.TrimSpace(customCharset.value)
		if value == "" && idx == 0 {
			value = strings.TrimSpace(customCharset.alias)
		}
		if value == "" {
			continue
		}
		keyspace.Options = append(keyspace.Options, customCharset.flag, value)
	}

	if contract.Skip != nil {
		if *contract.Skip < 0 {
			return nil, fmt.Errorf("%w: mask_segment skip must be non-negative", ErrInvalidKeyspaceContract)
		}
		keyspace.Skip = *contract.Skip
	}
	if contract.Limit != nil {
		if *contract.Limit <= 0 {
			return nil, fmt.Errorf("%w: mask_segment limit must be positive", ErrInvalidKeyspaceContract)
		}
		keyspace.Limit = *contract.Limit
	}

	attackMode := 3
	keyspace.AttackMode = &attackMode
	keyspace.Inputs = append(keyspace.Inputs, mask)
	return keyspace, nil
}

func writeCandidateWordlist(jobID string, candidates []string) (string, error) {
	baseDir := filepath.Join(os.TempDir(), "xfo-miner", "keyspace")
	if err := os.MkdirAll(baseDir, 0o755); err != nil {
		return "", fmt.Errorf("%w: mkdir keyspace dir: %v", ErrCandidateMaterializationFailed, err)
	}

	safeJobID := keyspaceJobIDSanitizer.ReplaceAllString(strings.TrimSpace(jobID), "_")
	if safeJobID == "" {
		safeJobID = "job"
	}

	filename := fmt.Sprintf("%s_%d.wordlist", safeJobID, time.Now().UnixNano())
	wordlistPath := filepath.Join(baseDir, filename)
	content := strings.Join(candidates, "\n") + "\n"
	if err := os.WriteFile(wordlistPath, []byte(content), 0o600); err != nil {
		return "", fmt.Errorf("%w: write wordlist: %v", ErrCandidateMaterializationFailed, err)
	}

	return wordlistPath, nil
}
