package scheduler

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/0xforce/xfo-miner/internal/pool"
)

var (
	ErrInvalidDictionaryContract    = errors.New("invalid_dictionary_contract")
	ErrUnsupportedDictionaryFormat  = errors.New("unsupported_dictionary_format")
	ErrDictionaryCacheResolveFailed = errors.New("dictionary_cache_resolve_failed")
)

var dictionaryChecksumSHA256Regex = regexp.MustCompile(`^[a-f0-9]{64}$`)

type keyspaceContractTypeOnly struct {
	Type string `json:"type"`
}

func validateDictionaryAdmission(job *pool.JobGPUMessage) error {
	if job == nil {
		return fmt.Errorf("%w: missing job payload", ErrInvalidDictionaryContract)
	}

	keyspaceType, err := readKeyspaceContractType(job.KeyspaceContract)
	if err != nil {
		// let existing keyspace validation/reporting handle malformed raw payload semantics
		return nil
	}

	hasDictionarySlice := strings.EqualFold(keyspaceType, "dictionary_slice")
	if job.Dictionary == nil {
		if hasDictionarySlice {
			return fmt.Errorf("%w: dictionary_slice requires dictionary payload", ErrInvalidDictionaryContract)
		}
		return nil
	}

	dict := job.Dictionary
	if strings.TrimSpace(dict.DictID) == "" {
		return fmt.Errorf("%w: missing dict_id", ErrInvalidDictionaryContract)
	}
	if strings.TrimSpace(dict.DictURL) == "" {
		return fmt.Errorf("%w: missing dict_url", ErrInvalidDictionaryContract)
	}
	compressFormat := strings.ToLower(strings.TrimSpace(dict.CompressFormat))
	if compressFormat == "" {
		return fmt.Errorf("%w: missing compress_format", ErrInvalidDictionaryContract)
	}
	if compressFormat != "lzma" {
		return fmt.Errorf("%w: %s", ErrUnsupportedDictionaryFormat, compressFormat)
	}
	checksum := strings.ToLower(strings.TrimSpace(dict.Checksum))
	if !dictionaryChecksumSHA256Regex.MatchString(checksum) {
		return fmt.Errorf("%w: invalid checksum", ErrInvalidDictionaryContract)
	}

	if !hasDictionarySlice {
		return fmt.Errorf("%w: dictionary payload requires dictionary_slice keyspace_contract", ErrInvalidDictionaryContract)
	}

	return nil
}

func readKeyspaceContractType(raw json.RawMessage) (string, error) {
	if len(raw) == 0 {
		return "", nil
	}
	var contract keyspaceContractTypeOnly
	if err := json.Unmarshal(raw, &contract); err != nil {
		return "", err
	}
	return strings.TrimSpace(contract.Type), nil
}
