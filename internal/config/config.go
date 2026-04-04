package config

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
)

// Config maps specs §4 config.json.
type Config struct {
	NodeID            string           `json:"node_id"`
	WalletAddress     string           `json:"wallet_address"`
	WorkerName        string           `json:"worker_name"`
	HostPlatformID    string           `json:"host_platform_id,omitempty"`
	PersistentMinerID string           `json:"persistent_miner_id,omitempty"`
	PoolURL           string           `json:"pool_url"`
	HashcatPath       string           `json:"hashcat_path"`
	MaxCPUThreads     int              `json:"max_cpu_threads"`
	AutoUpdate        AutoUpdateConfig `json:"auto_update"`
	IdleBehavior      IdleBehavior     `json:"idle_behavior"`
	CPUMining         CPUMiningConfig  `json:"cpu_mining"`
	IdentityMode      string           `json:"identity_mode,omitempty"`
	identityStatePath string           `json:"-"`
}

type identityState struct {
	HostPlatformID    string `json:"host_platform_id"`
	PersistentMinerID string `json:"persistent_miner_id"`
	OldWorkerName     string `json:"old_worker_name,omitempty"`
	MigrationCompleted bool  `json:"migration_completed"`
}

type AutoUpdateConfig struct {
	Enabled         bool   `json:"enabled"`
	CDNURL          string `json:"cdn_url"`
	PollIntervalSec int    `json:"poll_interval_sec"`
	JitterMaxSec    int    `json:"jitter_max_sec"`
}

const (
	defaultAutoUpdateCDNURL          = "https://update.xfo.network/releases/latest.json"
	defaultAutoUpdatePollIntervalSec = 14400
	defaultAutoUpdateJitterMaxSec    = 1800
)

type IdleBehavior struct {
	Enabled        bool   `json:"enabled"`
	GracePeriodSec int    `json:"grace_period_sec"`
	Command        string `json:"command"`
	Args           string `json:"args"`
}

type CPUMiningConfig struct {
	Enabled           bool   `json:"enabled"`
	XMRigPath         string `json:"xmrig_path"`
	StratumURL        string `json:"stratum_url"`
	MaxThreads        int    `json:"max_threads"`
	BackgroundThreads int    `json:"background_threads"`
}

func (c *Config) L2Enabled() bool {
	return strings.TrimSpace(c.PoolURL) != ""
}

func (c *Config) IdentityStatePath() string {
	return c.identityStatePath
}

func (c *Config) SetIdentityStatePath(path string) {
	c.identityStatePath = strings.TrimSpace(path)
}

func generateNodeID(walletAddress, workerName string) string {
	seed := strings.TrimSpace(walletAddress) + "|" + strings.TrimSpace(workerName)
	sum := sha256.Sum256([]byte(seed))
	return hex.EncodeToString(sum[:])[:12]
}

func isValidXfoAddress(addr string) bool {
	if len(addr) < 95 || len(addr) > 106 {
		return false
	}
	if !strings.HasPrefix(addr, "XFo") && !strings.HasPrefix(addr, "XFs") {
		return false
	}
	return true
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
	if err := cfg.ensureStableIdentity(path); err != nil {
		return nil, err
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	return &cfg, nil
}

func (c *Config) ensureStableIdentity(configPath string) error {
	statePath := filepath.Clean(configPath) + ".identity_state.json"
	c.identityStatePath = statePath
	state := identityState{}
	if raw, err := os.ReadFile(statePath); err == nil {
		_ = json.Unmarshal(raw, &state)
	}

	if strings.TrimSpace(c.PersistentMinerID) == "" {
		c.PersistentMinerID = strings.TrimSpace(state.PersistentMinerID)
	}
	if strings.TrimSpace(c.HostPlatformID) == "" {
		c.HostPlatformID = strings.TrimSpace(state.HostPlatformID)
	}

	if strings.TrimSpace(c.PersistentMinerID) == "" {
		id, err := newRandomID()
		if err != nil {
			return fmt.Errorf("generate persistent_miner_id: %w", err)
		}
		c.PersistentMinerID = id
	}
	if strings.TrimSpace(c.HostPlatformID) == "" {
		c.HostPlatformID = detectHostPlatformID()
	}

	if strings.TrimSpace(state.OldWorkerName) == "" {
		state.OldWorkerName = strings.TrimSpace(c.WorkerName)
	}

	if strings.TrimSpace(c.HostPlatformID) == "" {
		c.IdentityMode = "legacy_host"
	} else {
		c.IdentityMode = "stable"
	}

	state.HostPlatformID = c.HostPlatformID
	state.PersistentMinerID = c.PersistentMinerID
	raw, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal identity state: %w", err)
	}
	if err := os.WriteFile(statePath, raw, 0o600); err != nil {
		return fmt.Errorf("write identity state: %w", err)
	}

	return nil
}

func detectHostPlatformID() string {
	candidates := []string{"/etc/machine-id", "/var/lib/dbus/machine-id"}
	for _, p := range candidates {
		raw, err := os.ReadFile(p)
		if err != nil {
			continue
		}
		value := strings.ToLower(strings.TrimSpace(string(raw)))
		if value != "" {
			sum := sha256.Sum256([]byte(value))
			return hex.EncodeToString(sum[:16])
		}
	}

	hostname, err := os.Hostname()
	if err == nil {
		hostname = strings.TrimSpace(strings.ToLower(hostname))
		if hostname != "" {
			sum := sha256.Sum256([]byte(hostname))
			return hex.EncodeToString(sum[:16])
		}
	}

	return ""
}

func newRandomID() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

func (c *Config) applyDefaults() error {
	if strings.TrimSpace(c.NodeID) == "" {
		c.NodeID = generateNodeID(c.WalletAddress, c.WorkerName)
	}

	if strings.TrimSpace(c.HashcatPath) == "" {
		c.HashcatPath = "hashcat"
	}

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

	if strings.TrimSpace(c.AutoUpdate.CDNURL) == "" {
		c.AutoUpdate.CDNURL = defaultAutoUpdateCDNURL
	}
	if c.AutoUpdate.PollIntervalSec <= 0 {
		c.AutoUpdate.PollIntervalSec = defaultAutoUpdatePollIntervalSec
	}
	if c.AutoUpdate.JitterMaxSec <= 0 {
		c.AutoUpdate.JitterMaxSec = defaultAutoUpdateJitterMaxSec
	}

	return nil
}

func (c *Config) Validate() error {
	var validationErrs []error

	if strings.TrimSpace(c.WorkerName) == "" {
		validationErrs = append(validationErrs, errors.New("worker_name is required"))
	}
	if c.CPUMining.Enabled {
		walletAddress := strings.TrimSpace(c.WalletAddress)
		if walletAddress == "" {
			validationErrs = append(validationErrs, errors.New("wallet_address is required when cpu_mining.enabled is true"))
		} else if !isValidXfoAddress(walletAddress) {
			validationErrs = append(validationErrs, errors.New("wallet_address must be a valid XFo/XFs address (95-106 chars, base58)"))
		}
	}

	if c.L2Enabled() {
		walletAddress := strings.TrimSpace(c.WalletAddress)
		if walletAddress == "" {
			validationErrs = append(validationErrs, errors.New("wallet_address is required when pool_url is configured (L2 mode)"))
		} else if !isValidXfoAddress(walletAddress) {
			validationErrs = append(validationErrs, errors.New("wallet_address must be a valid XFo/XFs address (95-106 chars, base58)"))
		}

		pool, err := url.Parse(c.PoolURL)
		if err != nil {
			validationErrs = append(validationErrs, fmt.Errorf("pool_url invalid: %w", err))
		} else if (pool.Scheme != "wss" && pool.Scheme != "ws") || pool.Host == "" {
			validationErrs = append(validationErrs, errors.New("pool_url must be a valid wss:// or ws:// URL"))
		}
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
		if strings.TrimSpace(c.CPUMining.StratumURL) == "" {
			validationErrs = append(validationErrs, errors.New("cpu_mining.stratum_url is required when cpu_mining.enabled is true (e.g. stratum+tcp://host:3333)"))
		}
		if c.CPUMining.BackgroundThreads < 1 {
			validationErrs = append(validationErrs, errors.New("cpu_mining.background_threads must be >= 1"))
		}
		if c.CPUMining.MaxThreads < c.CPUMining.BackgroundThreads {
			validationErrs = append(validationErrs, errors.New("cpu_mining.max_threads must be >= cpu_mining.background_threads"))
		}
	}

	if c.AutoUpdate.PollIntervalSec < 1 {
		validationErrs = append(validationErrs, errors.New("auto_update.poll_interval_sec must be >= 1"))
	}
	if c.AutoUpdate.JitterMaxSec < 0 {
		validationErrs = append(validationErrs, errors.New("auto_update.jitter_max_sec must be >= 0"))
	}
	cdnURL, err := url.Parse(c.AutoUpdate.CDNURL)
	if err != nil || cdnURL.Host == "" || (cdnURL.Scheme != "https" && cdnURL.Scheme != "http") {
		validationErrs = append(validationErrs, errors.New("auto_update.cdn_url must be a valid http(s) URL"))
	}

	if len(validationErrs) > 0 {
		return errors.Join(validationErrs...)
	}
	return nil
}
