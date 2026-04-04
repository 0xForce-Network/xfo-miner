package identity

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

type state struct {
	PersistentMinerID string `json:"persistent_miner_id,omitempty"`
	OldWorkerName      string `json:"old_worker_name,omitempty"`
	MigrationCompleted bool   `json:"migration_completed"`
}

func NeedsMigrationClaim(identityPath string) (bool, string, error) {
	path := strings.TrimSpace(identityPath)
	if path == "" {
		return false, "", nil
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return false, "", nil
		}
		return false, "", fmt.Errorf("read identity state: %w", err)
	}

	var st state
	if err := json.Unmarshal(raw, &st); err != nil {
		return false, "", fmt.Errorf("decode identity state: %w", err)
	}

	if st.MigrationCompleted {
		return false, "", nil
	}

	if strings.TrimSpace(st.PersistentMinerID) == "" {
		return false, "", nil
	}

	oldWorkerName := strings.TrimSpace(st.OldWorkerName)
	if oldWorkerName == "" {
		return false, "", nil
	}

	return true, oldWorkerName, nil
}

func MarkMigrationCompleted(identityPath string) error {
	path := strings.TrimSpace(identityPath)
	if path == "" {
		return nil
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("read identity state: %w", err)
	}

	var payload map[string]json.RawMessage
	if err := json.Unmarshal(raw, &payload); err != nil {
		return fmt.Errorf("decode identity state payload: %w", err)
	}

	payload["migration_completed"] = json.RawMessage("true")

	encoded, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return fmt.Errorf("encode identity state payload: %w", err)
	}

	if err := os.WriteFile(path, encoded, 0o600); err != nil {
		return fmt.Errorf("write identity state: %w", err)
	}

	return nil
}
