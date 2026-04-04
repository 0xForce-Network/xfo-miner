package identity

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestNeedsMigrationClaim(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	path := filepath.Join(tempDir, "state.json")
	raw := []byte(`{"persistent_miner_id":"abc123","old_worker_name":"worker-legacy","migration_completed":false}`)
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatalf("write state: %v", err)
	}

	needs, oldWorker, err := NeedsMigrationClaim(path)
	if err != nil {
		t.Fatalf("NeedsMigrationClaim() error = %v", err)
	}
	if !needs {
		t.Fatalf("expected needs migration claim")
	}
	if oldWorker != "worker-legacy" {
		t.Fatalf("unexpected old worker name: %q", oldWorker)
	}
}

func TestNeedsMigrationClaimWithoutPersistentMinerID(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	path := filepath.Join(tempDir, "state.json")
	raw := []byte(`{"old_worker_name":"worker-legacy","migration_completed":false}`)
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatalf("write state: %v", err)
	}

	needs, oldWorker, err := NeedsMigrationClaim(path)
	if err != nil {
		t.Fatalf("NeedsMigrationClaim() error = %v", err)
	}
	if needs {
		t.Fatalf("expected no migration claim when persistent_miner_id is missing")
	}
	if oldWorker != "" {
		t.Fatalf("expected empty old worker name when persistent_miner_id is missing, got %q", oldWorker)
	}
}

func TestNeedsMigrationClaimCompleted(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	path := filepath.Join(tempDir, "state.json")
	raw := []byte(`{"old_worker_name":"worker-legacy","migration_completed":true}`)
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatalf("write state: %v", err)
	}

	needs, oldWorker, err := NeedsMigrationClaim(path)
	if err != nil {
		t.Fatalf("NeedsMigrationClaim() error = %v", err)
	}
	if needs {
		t.Fatalf("expected no migration claim when completed")
	}
	if oldWorker != "" {
		t.Fatalf("expected empty old worker name when completed, got %q", oldWorker)
	}
}

func TestMarkMigrationCompleted(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	path := filepath.Join(tempDir, "state.json")
	raw := []byte(`{"persistent_miner_id":"abc123","old_worker_name":"worker-legacy","migration_completed":false}`)
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatalf("write state: %v", err)
	}

	if err := MarkMigrationCompleted(path); err != nil {
		t.Fatalf("MarkMigrationCompleted() error = %v", err)
	}

	updatedRaw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read state: %v", err)
	}
	var state map[string]any
	if err := json.Unmarshal(updatedRaw, &state); err != nil {
		t.Fatalf("decode state: %v", err)
	}
	if state["migration_completed"] != true {
		t.Fatalf("expected migration_completed=true, got %#v", state["migration_completed"])
	}
	if state["persistent_miner_id"] != "abc123" {
		t.Fatalf("expected persistent_miner_id preserved")
	}
}
