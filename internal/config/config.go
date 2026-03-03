package config

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"runtime"
	"strings"
)

// Config maps specs §4 config.json.
type Config struct {
	NodeID        string           `json:"node_id"`
	WorkerName    string           `json:"worker_name"`
	PoolURL       string           `json:"pool_url"`
	MaxCPUThreads int              `json:"max_cpu_threads"`
	Multisig      MultisigConfig   `json:"multisig"`
	AutoUpdate    AutoUpdateConfig `json:"auto_update"`
	IdleBehavior  IdleBehavior     `json:"idle_behavior"`
	CPUMining     CPUMiningConfig  `json:"cpu_mining"`
}

type MultisigConfig struct {
	Enabled      bool   `json:"enabled"`
	WalletRPCURL string `json:"wallet_rpc_url"`
	OracleAPIURL string `json:"oracle_api_url"`
}

type AutoUpdateConfig struct {
	Enabled bool `json:"enabled"`
}

type IdleBehavior struct {
	Enabled        bool   `json:"enabled"`
	GracePeriodSec int    `json:"grace_period_sec"`
	Command        string `json:"command"`
	Args           string `json:"args"`
}

type CPUMiningConfig struct {
	Enabled           bool   `json:"enabled"`
	XMRigPath         string `json:"xmrig_path"`
	MaxThreads        int    `json:"max_threads"`
	BackgroundThreads int    `json:"background_threads"`
}

func LoadConfig(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}

	var cfg Config
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("decode config: %w", err)
	}

	if err := cfg.applyDefaults(); err != nil {
		return nil, err
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	return &cfg, nil
}

func (c *Config) applyDefaults() error {
	if c.MaxCPUThreads == 0 {
		cpu := runtime.NumCPU() / 2
		if cpu < 1 {
			cpu = 1
		}
		c.MaxCPUThreads = cpu
	}
	if c.MaxCPUThreads < 0 {
		return errors.New("max_cpu_threads cannot be negative")
	}

	if c.CPUMining.BackgroundThreads == 0 {
		c.CPUMining.BackgroundThreads = 1
	}
	if c.CPUMining.MaxThreads == 0 {
		c.CPUMining.MaxThreads = c.MaxCPUThreads
	}

	return nil
}

func (c *Config) Validate() error {
	var validationErrs []error

	if strings.TrimSpace(c.NodeID) == "" {
		validationErrs = append(validationErrs, errors.New("node_id is required"))
	}
	if strings.TrimSpace(c.WorkerName) == "" {
		validationErrs = append(validationErrs, errors.New("worker_name is required"))
	}

	pool, err := url.Parse(c.PoolURL)
	if err != nil {
		validationErrs = append(validationErrs, fmt.Errorf("pool_url invalid: %w", err))
	} else if pool.Scheme != "wss" || pool.Host == "" {
		validationErrs = append(validationErrs, errors.New("pool_url must be a valid wss:// URL"))
	}

	if c.MaxCPUThreads < 1 {
		validationErrs = append(validationErrs, errors.New("max_cpu_threads must be >= 1"))
	}
	if c.MaxCPUThreads > runtime.NumCPU() {
		validationErrs = append(validationErrs, fmt.Errorf("max_cpu_threads (%d) cannot exceed runtime CPUs (%d)", c.MaxCPUThreads, runtime.NumCPU()))
	}

	if c.IdleBehavior.GracePeriodSec < 0 {
		validationErrs = append(validationErrs, errors.New("idle_behavior.grace_period_sec cannot be negative"))
	}
	if c.IdleBehavior.Enabled && strings.TrimSpace(c.IdleBehavior.Command) == "" {
		validationErrs = append(validationErrs, errors.New("idle_behavior.command is required when idle_behavior.enabled is true"))
	}

	if c.CPUMining.Enabled {
		if strings.TrimSpace(c.CPUMining.XMRigPath) == "" {
			validationErrs = append(validationErrs, errors.New("cpu_mining.xmrig_path is required when cpu_mining.enabled is true"))
		}
		if c.CPUMining.BackgroundThreads < 1 {
			validationErrs = append(validationErrs, errors.New("cpu_mining.background_threads must be >= 1"))
		}
		if c.CPUMining.MaxThreads < c.CPUMining.BackgroundThreads {
			validationErrs = append(validationErrs, errors.New("cpu_mining.max_threads must be >= cpu_mining.background_threads"))
		}
	}

	if c.Multisig.Enabled {
		rpcURL, err := url.Parse(c.Multisig.WalletRPCURL)
		if err != nil || rpcURL.Host == "" || (rpcURL.Scheme != "http" && rpcURL.Scheme != "https") {
			validationErrs = append(validationErrs, errors.New("multisig.wallet_rpc_url must be a valid http(s) URL when multisig.enabled is true"))
		}
		oracleURL, err := url.Parse(c.Multisig.OracleAPIURL)
		if err != nil || oracleURL.Host == "" || (oracleURL.Scheme != "http" && oracleURL.Scheme != "https") {
			validationErrs = append(validationErrs, errors.New("multisig.oracle_api_url must be a valid http(s) URL when multisig.enabled is true"))
		}
	}

	if len(validationErrs) > 0 {
		return errors.Join(validationErrs...)
	}
	return nil
}
