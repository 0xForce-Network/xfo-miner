package config

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func TestLoadConfigAppliesDefaultCPUThreads(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	path := filepath.Join(tempDir, "config.json")
	content := `{
	  "node_id": "node-1",
	  "wallet_address": "",
	  "worker_name": "worker-1",
	  "pool_url": "wss://pool.example.com/ws",
	  "max_cpu_threads": 0,
	  "multisig": {
	    "enabled": false,
	    "wallet_rpc_url": "",
	    "oracle_api_url": ""
	  },
	  "auto_update": {
	    "enabled": true
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
	  "multisig": {
	    "enabled": false,
	    "wallet_rpc_url": "",
	    "oracle_api_url": ""
	  },
	  "auto_update": {
	    "enabled": false
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
		t.Fatalf("expected validation error for invalid config")
	}
}

func TestLoadConfigCPUMiningDefaults(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	path := filepath.Join(tempDir, "cpu-mining-defaults.json")
	content := `{
	  "node_id": "node-1",
	  "wallet_address": "XFo27t1JjPjWFmmk558cEWJC8HRjQJuHTRD34nMksE3nR2j6DxuxE3XTeRuVf8c3hqctQNgTWEiYp2AdMK1HunyJ3jb9Nta5W3",
	  "worker_name": "worker-1",
	  "pool_url": "wss://pool.example.com/ws",
	  "max_cpu_threads": 4,
	  "multisig": {
	    "enabled": false,
	    "wallet_rpc_url": "",
	    "oracle_api_url": ""
	  },
	  "auto_update": {
	    "enabled": true
	  },
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
	}`

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
	if cfg.CPUMining.MaxThreads != 4 {
		t.Fatalf("unexpected max threads: got %d want 4", cfg.CPUMining.MaxThreads)
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
	  "multisig": {
	    "enabled": false,
	    "wallet_rpc_url": "",
	    "oracle_api_url": ""
	  },
	  "auto_update": {
	    "enabled": true
	  },
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
	  "multisig": {
	    "enabled": false,
	    "wallet_rpc_url": "",
	    "oracle_api_url": ""
	  },
	  "auto_update": {
	    "enabled": true
	  },
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

func TestLoadConfigMultisigValidation(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	path := filepath.Join(tempDir, "multisig-invalid.json")
	content := `{
	  "node_id": "node-1",
	  "wallet_address": "",
	  "worker_name": "worker-1",
	  "pool_url": "wss://pool.example.com/ws",
	  "max_cpu_threads": 4,
	  "multisig": {
	    "enabled": true,
	    "wallet_rpc_url": "not-a-url",
	    "oracle_api_url": "also-not-url"
	  },
	  "auto_update": {
	    "enabled": true
	  },
	  "cpu_mining": {
	    "enabled": false,
	    "xmrig_path": "",
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
		t.Fatalf("expected validation error for multisig urls")
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
	  "multisig": {
	    "enabled": false,
	    "wallet_rpc_url": "",
	    "oracle_api_url": ""
	  },
	  "auto_update": {
	    "enabled": true
	  },
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

func TestLoadConfigEmptyPoolURLAllowed(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	path := filepath.Join(tempDir, "pool-url-empty.json")
	content := `{
	  "node_id": "node-1",
	  "wallet_address": "",
	  "worker_name": "worker-1",
	  "pool_url": "",
	  "max_cpu_threads": 4,
	  "multisig": {
	    "enabled": false,
	    "wallet_rpc_url": "",
	    "oracle_api_url": ""
	  },
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
	  "multisig": {
	    "enabled": false,
	    "wallet_rpc_url": "",
	    "oracle_api_url": ""
	  },
	  "auto_update": {
	    "enabled": true
	  },
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
