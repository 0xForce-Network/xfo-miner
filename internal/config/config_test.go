package config

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestLoadConfigAppliesDefaultCPUThreads(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	path := filepath.Join(tempDir, "config.json")
	content := `{
	  "node_id": "node-1",
	  "wallet_address": "XFo27t1JjPjWFmmk558cEWJC8HRjQJuHTRD34nMksE3nR2j6DxuxE3XTeRuVf8c3hqctQNgTWEiYp2AdMK1HunyJ3jb9Nta5W3",
	  "worker_name": "worker-1",
	  "pool_url": "wss://pool.example.com/ws",
	  "max_cpu_threads": 0,
	  "idle_behavior": {
	    "enabled": false,
	    "grace_period_sec": 0,
	    "command": "",
	    "args": ""
	  }
	}`

	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write config file: %v", err)
	}

	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig() error = %v", err)
	}

	expected := runtime.NumCPU() / 2
	if expected < 1 {
		expected = 1
	}
	if cfg.MaxCPUThreads != expected {
		t.Fatalf("unexpected MaxCPUThreads: got %d want %d", cfg.MaxCPUThreads, expected)
	}
}

func TestLoadConfigValidationErrors(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	path := filepath.Join(tempDir, "invalid.json")
	content := `{
	  "node_id": "",
	  "wallet_address": "",
	  "worker_name": "worker-1",
	  "pool_url": "http://pool.example.com/ws",
	  "max_cpu_threads": 1,
	  "idle_behavior": {
	    "enabled": false,
	    "grace_period_sec": 0,
	    "command": "",
	    "args": ""
	  }
	}`

	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write config file: %v", err)
	}

	if _, err := LoadConfig(path); err == nil {
		t.Fatalf("expected validation error for invalid config")
	}
}

func TestLoadConfigCPUMiningDefaults(t *testing.T) {
	t.Parallel()

	maxCPUThreads := runtime.NumCPU()
	if maxCPUThreads < 1 {
		maxCPUThreads = 1
	}

	tempDir := t.TempDir()
	path := filepath.Join(tempDir, "cpu-mining-defaults.json")
	content := fmt.Sprintf(`{
	  "node_id": "node-1",
	  "wallet_address": "XFo27t1JjPjWFmmk558cEWJC8HRjQJuHTRD34nMksE3nR2j6DxuxE3XTeRuVf8c3hqctQNgTWEiYp2AdMK1HunyJ3jb9Nta5W3",
	  "worker_name": "worker-1",
	  "pool_url": "wss://pool.example.com/ws",
	  "max_cpu_threads": %d,
	  "cpu_mining": {
	    "enabled": true,
	    "xmrig_path": "./bin/xmrig",
	    "stratum_url": "stratum+tcp://pool.example.com:3333",
	    "max_threads": 0,
	    "background_threads": 0
	  },
	  "idle_behavior": {
	    "enabled": false,
	    "grace_period_sec": 0,
	    "command": "",
	    "args": ""
	  }
	}`, maxCPUThreads)

	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write config file: %v", err)
	}

	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig() error = %v", err)
	}

	if cfg.CPUMining.BackgroundThreads != 1 {
		t.Fatalf("unexpected background threads: got %d want 1", cfg.CPUMining.BackgroundThreads)
	}
	if cfg.CPUMining.MaxThreads != maxCPUThreads {
		t.Fatalf("unexpected max threads: got %d want %d", cfg.CPUMining.MaxThreads, maxCPUThreads)
	}
}

func TestLoadConfigCPUMiningValidationBackgroundMinimum(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	path := filepath.Join(tempDir, "cpu-mining-invalid-background.json")
	content := `{
	  "node_id": "node-1",
	  "wallet_address": "XFo27t1JjPjWFmmk558cEWJC8HRjQJuHTRD34nMksE3nR2j6DxuxE3XTeRuVf8c3hqctQNgTWEiYp2AdMK1HunyJ3jb9Nta5W3",
	  "worker_name": "worker-1",
	  "pool_url": "wss://pool.example.com/ws",
	  "max_cpu_threads": 4,
	  "cpu_mining": {
	    "enabled": true,
	    "xmrig_path": "./bin/xmrig",
	    "stratum_url": "stratum+tcp://pool.example.com:3333",
	    "max_threads": 4,
	    "background_threads": -1
	  },
	  "idle_behavior": {
	    "enabled": false,
	    "grace_period_sec": 0,
	    "command": "",
	    "args": ""
	  }
	}`

	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write config file: %v", err)
	}

	if _, err := LoadConfig(path); err == nil {
		t.Fatalf("expected validation error for background_threads < 1")
	}
}

func TestLoadConfigCPUMiningValidationMaxGTEBackground(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	path := filepath.Join(tempDir, "cpu-mining-invalid-max.json")
	content := `{
	  "node_id": "node-1",
	  "wallet_address": "XFo27t1JjPjWFmmk558cEWJC8HRjQJuHTRD34nMksE3nR2j6DxuxE3XTeRuVf8c3hqctQNgTWEiYp2AdMK1HunyJ3jb9Nta5W3",
	  "worker_name": "worker-1",
	  "pool_url": "wss://pool.example.com/ws",
	  "max_cpu_threads": 4,
	  "cpu_mining": {
	    "enabled": true,
	    "xmrig_path": "./bin/xmrig",
	    "stratum_url": "stratum+tcp://pool.example.com:3333",
	    "max_threads": 1,
	    "background_threads": 2
	  },
	  "idle_behavior": {
	    "enabled": false,
	    "grace_period_sec": 0,
	    "command": "",
	    "args": ""
	  }
	}`

	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write config file: %v", err)
	}

	if _, err := LoadConfig(path); err == nil {
		t.Fatalf("expected validation error when max_threads < background_threads")
	}
}

func TestLoadConfigCPUMiningExtraArgsTrimmed(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	path := filepath.Join(tempDir, "cpu-mining-extra-args.json")
	content := `{
	  "node_id": "node-1",
	  "wallet_address": "XFo27t1JjPjWFmmk558cEWJC8HRjQJuHTRD34nMksE3nR2j6DxuxE3XTeRuVf8c3hqctQNgTWEiYp2AdMK1HunyJ3jb9Nta5W3",
	  "worker_name": "worker-1",
	  "pool_url": "wss://pool.example.com/ws",
	  "max_cpu_threads": 4,
	  "cpu_mining": {
	    "enabled": true,
	    "xmrig_path": "./bin/xmrig",
	    "stratum_url": "stratum+tcp://pool.example.com:3333",
	    "max_threads": 4,
	    "background_threads": 1,
	    "extra_args": ["  --proxy=127.0.0.1:1080  ", " --keepalive "]
	  },
	  "idle_behavior": {
	    "enabled": false,
	    "grace_period_sec": 0,
	    "command": "",
	    "args": ""
	  }
	}`

	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write config file: %v", err)
	}

	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig() error = %v", err)
	}

	if len(cfg.CPUMining.ExtraArgs) != 2 {
		t.Fatalf("unexpected extra args length: got %d want 2", len(cfg.CPUMining.ExtraArgs))
	}
	if cfg.CPUMining.ExtraArgs[0] != "--proxy=127.0.0.1:1080" {
		t.Fatalf("unexpected first extra arg: got %q", cfg.CPUMining.ExtraArgs[0])
	}
	if cfg.CPUMining.ExtraArgs[1] != "--keepalive" {
		t.Fatalf("unexpected second extra arg: got %q", cfg.CPUMining.ExtraArgs[1])
	}
}

func TestLoadConfigCPUMiningValidationRejectsReservedExtraArgs(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	path := filepath.Join(tempDir, "cpu-mining-invalid-extra-args.json")
	content := `{
	  "node_id": "node-1",
	  "wallet_address": "XFo27t1JjPjWFmmk558cEWJC8HRjQJuHTRD34nMksE3nR2j6DxuxE3XTeRuVf8c3hqctQNgTWEiYp2AdMK1HunyJ3jb9Nta5W3",
	  "worker_name": "worker-1",
	  "pool_url": "wss://pool.example.com/ws",
	  "max_cpu_threads": 4,
	  "cpu_mining": {
	    "enabled": true,
	    "xmrig_path": "./bin/xmrig",
	    "stratum_url": "stratum+tcp://pool.example.com:3333",
	    "max_threads": 4,
	    "background_threads": 1,
	    "extra_args": ["--http-port=17777"]
	  },
	  "idle_behavior": {
	    "enabled": false,
	    "grace_period_sec": 0,
	    "command": "",
	    "args": ""
	  }
	}`

	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write config file: %v", err)
	}

	_, err := LoadConfig(path)
	if err == nil {
		t.Fatalf("expected validation error for reserved cpu_mining.extra_args")
	}
	if !strings.Contains(err.Error(), "reserved xmrig flag") {
		t.Fatalf("expected reserved flag validation error, got: %v", err)
	}
}

func TestLoadConfigWalletAddressValidation(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	path := filepath.Join(tempDir, "wallet-addr-invalid.json")
	content := `{
	  "node_id": "node-1",
	  "wallet_address": "XFo_too_short",
	  "worker_name": "worker-1",
	  "pool_url": "wss://pool.example.com/ws",
	  "max_cpu_threads": 4,
	  "cpu_mining": {
	    "enabled": true,
	    "xmrig_path": "./bin/xmrig",
	    "stratum_url": "stratum+tcp://pool.example.com:3333",
	    "max_threads": 4,
	    "background_threads": 1
	  },
	  "idle_behavior": {
	    "enabled": false,
	    "grace_period_sec": 0,
	    "command": "",
	    "args": ""
	  }
	}`

	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write config file: %v", err)
	}

	if _, err := LoadConfig(path); err == nil {
		t.Fatalf("expected validation error for invalid wallet_address")
	}
}

func TestLoadConfigL2RequiresWalletAddress(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	path := filepath.Join(tempDir, "l2-wallet-required.json")
	content := `{
	  "node_id": "node-1",
	  "wallet_address": "",
	  "worker_name": "worker-1",
	  "pool_url": "wss://pool.example.com/ws",
	  "max_cpu_threads": 2,
	  "cpu_mining": {
	    "enabled": false,
	    "xmrig_path": "",
	    "stratum_url": "",
	    "max_threads": 0,
	    "background_threads": 0
	  },
	  "idle_behavior": {
	    "enabled": false,
	    "grace_period_sec": 0,
	    "command": "",
	    "args": ""
	  }
	}`

	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write config file: %v", err)
	}

	if _, err := LoadConfig(path); err == nil {
		t.Fatalf("expected validation error for missing wallet_address in L2 mode")
	}
}

func TestLoadConfigL2WalletAddressValidation(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	path := filepath.Join(tempDir, "l2-wallet-invalid.json")
	content := `{
	  "node_id": "node-1",
	  "wallet_address": "XFo_too_short",
	  "worker_name": "worker-1",
	  "pool_url": "wss://pool.example.com/ws",
	  "max_cpu_threads": 2,
	  "cpu_mining": {
	    "enabled": false,
	    "xmrig_path": "",
	    "stratum_url": "",
	    "max_threads": 0,
	    "background_threads": 0
	  },
	  "idle_behavior": {
	    "enabled": false,
	    "grace_period_sec": 0,
	    "command": "",
	    "args": ""
	  }
	}`

	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write config file: %v", err)
	}

	if _, err := LoadConfig(path); err == nil {
		t.Fatalf("expected validation error for invalid wallet_address in L2 mode")
	}
}

func TestLoadConfigEmptyPoolURLAllowed(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	path := filepath.Join(tempDir, "pool-url-empty.json")
	content := `{
	  "node_id": "node-1",
	  "wallet_address": "",
	  "worker_name": "worker-1",
	  "pool_url": "",
	  "max_cpu_threads": 1,
	  "cpu_mining": {
	    "enabled": false,
	    "xmrig_path": "",
	    "stratum_url": "",
	    "max_threads": 0,
	    "background_threads": 0
	  },
	  "idle_behavior": {
	    "enabled": false,
	    "grace_period_sec": 0,
	    "command": "",
	    "args": ""
	  }
	}`

	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write config file: %v", err)
	}

	if _, err := LoadConfig(path); err != nil {
		t.Fatalf("LoadConfig() error = %v", err)
	}
}

func TestLoadConfigL2EnabledFlag(t *testing.T) {
	t.Parallel()

	cfgWithPool := &Config{PoolURL: "wss://pool.example.com/ws"}
	if !cfgWithPool.L2Enabled() {
		t.Fatalf("expected L2Enabled()=true when pool_url is set")
	}

	cfgWithoutPool := &Config{PoolURL: "   "}
	if cfgWithoutPool.L2Enabled() {
		t.Fatalf("expected L2Enabled()=false when pool_url is empty")
	}
}

func TestLoadConfigNodeIDAutoGeneratedWhenEmpty(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	path := filepath.Join(tempDir, "node-id-auto.json")
	content := `{
	  "node_id": "",
	  "wallet_address": "XFo27t1JjPjWFmmk558cEWJC8HRjQJuHTRD34nMksE3nR2j6DxuxE3XTeRuVf8c3hqctQNgTWEiYp2AdMK1HunyJ3jb9Nta5W3",
	  "worker_name": "worker-1",
	  "pool_url": "",
	  "max_cpu_threads": 2,
	  "cpu_mining": {
	    "enabled": true,
	    "xmrig_path": "./bin/xmrig",
	    "stratum_url": "stratum+tcp://pool.example.com:3333",
	    "max_threads": 2,
	    "background_threads": 1
	  },
	  "idle_behavior": {
	    "enabled": false,
	    "grace_period_sec": 0,
	    "command": "",
	    "args": ""
	  }
	}`

	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write config file: %v", err)
	}

	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig() error = %v", err)
	}
	if len(cfg.NodeID) != 12 {
		t.Fatalf("expected auto-generated node_id length 12, got %q", cfg.NodeID)
	}
}

func TestLoadConfigRejectsLegacyAutoUpdateBlock(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	path := filepath.Join(tempDir, "legacy-auto-update.json")
	content := `{
	  "node_id": "node-1",
	  "wallet_address": "",
	  "worker_name": "worker-1",
	  "pool_url": "",
	  "max_cpu_threads": 2,
	  "auto_update": {
	    "enabled": true
	  },
	  "cpu_mining": {
	    "enabled": false,
	    "xmrig_path": "",
	    "stratum_url": "",
	    "max_threads": 0,
	    "background_threads": 0
	  },
	  "idle_behavior": {
	    "enabled": false,
	    "grace_period_sec": 0,
	    "command": "",
	    "args": ""
	  }
	}`

	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write config file: %v", err)
	}

	if _, err := LoadConfig(path); err == nil {
		t.Fatalf("expected decode error for legacy auto_update block")
	} else if !strings.Contains(err.Error(), "unknown field \"auto_update\"") {
		t.Fatalf("expected unknown auto_update field error, got %v", err)
	}
}

func TestLoadConfigRejectsRuntimeOwnedIdentityFields(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	path := filepath.Join(tempDir, "runtime-owned-identity.json")
	content := `{
	  "node_id": "node-1",
	  "wallet_address": "",
	  "worker_name": "worker-1",
	  "host_platform_id": "forged-host",
	  "persistent_miner_id": "forged-miner",
	  "pool_url": "",
	  "max_cpu_threads": 2,
	  "cpu_mining": {
	    "enabled": false,
	    "xmrig_path": "",
	    "stratum_url": "",
	    "max_threads": 0,
	    "background_threads": 0
	  },
	  "idle_behavior": {
	    "enabled": false,
	    "grace_period_sec": 0,
	    "command": "",
	    "args": ""
	  }
	}`

	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write config file: %v", err)
	}

	if _, err := LoadConfig(path); err == nil {
		t.Fatalf("expected decode error for runtime-owned identity fields")
	} else if !strings.Contains(err.Error(), "unknown field \"host_platform_id\"") {
		t.Fatalf("expected unknown host_platform_id field error, got %v", err)
	}
}

func TestLoadConfigHashcatPathDefault(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	path := filepath.Join(tempDir, "hashcat-path-default.json")
	content := `{
	  "node_id": "node-1",
	  "wallet_address": "",
	  "worker_name": "worker-1",
	  "pool_url": "",
	  "max_cpu_threads": 2,
	  "cpu_mining": {
	    "enabled": false,
	    "xmrig_path": "",
	    "stratum_url": "",
	    "max_threads": 0,
	    "background_threads": 0
	  },
	  "idle_behavior": {
	    "enabled": false,
	    "grace_period_sec": 0,
	    "command": "",
	    "args": ""
	  }
	}`

	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write config file: %v", err)
	}

	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig() error = %v", err)
	}

	if cfg.HashcatPath != "hashcat" {
		t.Fatalf("unexpected hashcat path default: got %q want %q", cfg.HashcatPath, "hashcat")
	}
}

func TestLoadConfigHashcatPathCustom(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	path := filepath.Join(tempDir, "hashcat-path-custom.json")
	content := `{
	  "node_id": "node-1",
	  "wallet_address": "",
	  "worker_name": "worker-1",
	  "pool_url": "",
	  "hashcat_path": "/opt/hashcat/hashcat",
	  "max_cpu_threads": 2,
	  "cpu_mining": {
	    "enabled": false,
	    "xmrig_path": "",
	    "stratum_url": "",
	    "max_threads": 0,
	    "background_threads": 0
	  },
	  "idle_behavior": {
	    "enabled": false,
	    "grace_period_sec": 0,
	    "command": "",
	    "args": ""
	  }
	}`

	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write config file: %v", err)
	}

	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig() error = %v", err)
	}

	if cfg.HashcatPath != "/opt/hashcat/hashcat" {
		t.Fatalf("unexpected hashcat path: got %q", cfg.HashcatPath)
	}
}

func TestLoadConfigStableIdentityGeneratedAndPersisted(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	path := filepath.Join(tempDir, "identity.json")
	content := `{
	  "node_id": "node-1",
	  "wallet_address": "XFo27t1JjPjWFmmk558cEWJC8HRjQJuHTRD34nMksE3nR2j6DxuxE3XTeRuVf8c3hqctQNgTWEiYp2AdMK1HunyJ3jb9Nta5W3",
	  "worker_name": "worker-1",
	  "pool_url": "",
	  "max_cpu_threads": 2,
	  "cpu_mining": {
	    "enabled": false,
	    "xmrig_path": "",
	    "stratum_url": "",
	    "max_threads": 0,
	    "background_threads": 0
	  },
	  "idle_behavior": {
	    "enabled": false,
	    "grace_period_sec": 0,
	    "command": "",
	    "args": ""
	  }
	}`

	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write config file: %v", err)
	}

	first, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig() first error = %v", err)
	}
	if first.PersistentMinerID == "" || first.HostPlatformID == "" {
		t.Fatalf("expected generated stable identities, got persistent=%q host=%q", first.PersistentMinerID, first.HostPlatformID)
	}
	if first.HostPlatformSource == "" {
		t.Fatalf("expected generated host_platform_source to be non-empty")
	}

	second, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig() second error = %v", err)
	}
	if first.PersistentMinerID != second.PersistentMinerID || first.HostPlatformID != second.HostPlatformID {
		t.Fatalf("expected persisted identities, first=(%s,%s) second=(%s,%s)", first.PersistentMinerID, first.HostPlatformID, second.PersistentMinerID, second.HostPlatformID)
	}
	if first.HostPlatformSource != second.HostPlatformSource {
		t.Fatalf("expected persisted host_platform_source, first=%s second=%s", first.HostPlatformSource, second.HostPlatformSource)
	}

	raw, err := os.ReadFile(first.IdentityStatePath())
	if err != nil {
		t.Fatalf("read identity state: %v", err)
	}
	var state struct {
		OldWorkerName      string `json:"old_worker_name"`
		MigrationCompleted bool   `json:"migration_completed"`
	}
	if err := json.Unmarshal(raw, &state); err != nil {
		t.Fatalf("decode identity state: %v", err)
	}
	if state.OldWorkerName != "worker-1" {
		t.Fatalf("expected old_worker_name worker-1, got %q", state.OldWorkerName)
	}
	if state.MigrationCompleted {
		t.Fatalf("expected migration_completed=false by default")
	}
}

func TestDetectHostPlatformIDWindowsMachineGUID(t *testing.T) {
	t.Parallel()

	origGOOS := hostPlatformGOOS
	origExec := hostPlatformExecCommandContext
	defer func() {
		hostPlatformGOOS = origGOOS
		hostPlatformExecCommandContext = origExec
	}()

	hostPlatformGOOS = "windows"
	hostPlatformExecCommandContext = func(_ context.Context, name string, _ ...string) *exec.Cmd {
		if name == "reg" {
			return exec.Command("/bin/sh", "-c", "cat <<'EOF'\nHKEY_LOCAL_MACHINE\\SOFTWARE\\Microsoft\\Cryptography\n    MachineGuid    REG_SZ    D1B90E6C-5B11-4A2F-ACF0-5EDB745EA4BB\nEOF")
		}
		return exec.Command("/bin/sh", "-c", "exit 1")
	}

	id, source := detectHostPlatformID()
	if id == "" {
		t.Fatalf("expected windows machine guid based host platform id")
	}
	if source != "windows_machine_guid" {
		t.Fatalf("expected source windows_machine_guid, got %q", source)
	}
}
