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

	tempDir := t.TempDir()
	path := filepath.Join(tempDir, "cpu-mining-defaults.json")
	content := `{
	  "node_id": "node-1",
	  "worker_name": "worker-1",
	  "pool_url": "wss://pool.example.com/ws",
	  "max_cpu_threads": 4,
	  "cpu_mining": {
	    "enabled": true,
	    "xmrig_path": "./bin/xmrig",
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
	  "worker_name": "worker-1",
	  "pool_url": "wss://pool.example.com/ws",
	  "max_cpu_threads": 4,
	  "cpu_mining": {
	    "enabled": true,
	    "xmrig_path": "./bin/xmrig",
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
	  "worker_name": "worker-1",
	  "pool_url": "wss://pool.example.com/ws",
	  "max_cpu_threads": 4,
	  "cpu_mining": {
	    "enabled": true,
	    "xmrig_path": "./bin/xmrig",
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
