package config

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

// Config maps specs §4 config.json.
type Config struct {
	NodeID             string          `json:"node_id"`
	WalletAddress      string          `json:"wallet_address"`
	WorkerName         string          `json:"worker_name"`
	HostPlatformID     string          `json:"-"`
	HostPlatformSource string          `json:"-"`
	PersistentMinerID  string          `json:"-"`
	PoolURL            string          `json:"pool_url"`
	HashcatPath        string          `json:"hashcat_path"`
	MaxCPUThreads      int             `json:"max_cpu_threads"`
	IdleBehavior       IdleBehavior    `json:"idle_behavior"`
	CPUMining          CPUMiningConfig `json:"cpu_mining"`
	IdentityMode       string          `json:"-"`
	identityStatePath  string          `json:"-"`
}

type identityState struct {
	HostPlatformID     string `json:"host_platform_id"`
	HostPlatformSource string `json:"host_platform_source,omitempty"`
	PersistentMinerID  string `json:"persistent_miner_id"`
	OldWorkerName      string `json:"old_worker_name,omitempty"`
	MigrationCompleted bool   `json:"migration_completed"`
}

var hostPlatformExecCommandContext = exec.CommandContext
var hostPlatformGOOS = runtime.GOOS
var executablePathNormalizeGOOS = runtime.GOOS
var validationGOOS = runtime.GOOS

type IdleBehavior struct {
	Enabled            bool     `json:"enabled"`
	GracePeriodSec     int      `json:"grace_period_sec"`
	RestartCooldownSec int      `json:"restart_cooldown_sec"`
	Command            string   `json:"command"`
	Args               string   `json:"args"`
	ArgsArray          []string `json:"args_array,omitempty"`
}

func (i *IdleBehavior) UnmarshalJSON(data []byte) error {
	type idleBehaviorRaw struct {
		Enabled            bool            `json:"enabled"`
		GracePeriodSec     int             `json:"grace_period_sec"`
		RestartCooldownSec int             `json:"restart_cooldown_sec"`
		Command            string          `json:"command"`
		Args               json.RawMessage `json:"args"`
		ArgsArray          []string        `json:"args_array"`
	}

	var raw idleBehaviorRaw
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&raw); err != nil {
		return err
	}

	*i = IdleBehavior{
		Enabled:            raw.Enabled,
		GracePeriodSec:     raw.GracePeriodSec,
		RestartCooldownSec: raw.RestartCooldownSec,
		Command:            raw.Command,
		ArgsArray:          append([]string(nil), raw.ArgsArray...),
	}

	if len(raw.Args) == 0 || bytes.Equal(bytes.TrimSpace(raw.Args), []byte("null")) {
		return nil
	}

	var legacyArgs string
	if err := json.Unmarshal(raw.Args, &legacyArgs); err == nil {
		i.Args = legacyArgs
		return nil
	}

	var argsArray []string
	if err := json.Unmarshal(raw.Args, &argsArray); err == nil {
		i.ArgsArray = append([]string(nil), argsArray...)
		return nil
	}

	return errors.New("idle_behavior.args must be a string or an array of strings")
}

type CPUMiningConfig struct {
	Enabled           bool     `json:"enabled"`
	XMRigPath         string   `json:"xmrig_path"`
	XMRigLogPath      string   `json:"xmrig_log_path,omitempty"`
	StratumURL        string   `json:"stratum_url"`
	MaxThreads        int      `json:"max_threads"`
	BackgroundThreads int      `json:"background_threads"`
	ExtraArgs         []string `json:"extra_args,omitempty"`
}

const defaultXMRigLogPath = "logs/xmrig.log"

var reservedXMRigFlags = map[string]struct{}{
	"-a":                     {},
	"--algo":                 {},
	"-c":                     {},
	"--config":               {},
	"--cpu-max-threads-hint": {},
	"--http-access-token":    {},
	"--http-enabled":         {},
	"--http-host":            {},
	"--http-no-restricted":   {},
	"--http-port":            {},
	"--log-file":             {},
	"--no-color":             {},
	"-o":                     {},
	"--pass":                 {},
	"-O":                     {},
	"--threads":              {},
	"-p":                     {},
	"--url":                  {},
	"--user":                 {},
	"--user-agent":           {},
	"--userpass":             {},
	"-t":                     {},
	"-u":                     {},
}

func classifyReservedXMRigFlag(token string) (string, bool) {
	trimmed := strings.TrimSpace(token)
	if trimmed == "" {
		return "", false
	}

	if idx := strings.Index(trimmed, "="); idx >= 0 {
		trimmed = trimmed[:idx]
	}

	_, ok := reservedXMRigFlags[trimmed]
	return trimmed, ok
}

func (c *CPUMiningConfig) ValidateExtraArgs() error {
	var validationErrs []error
	for i, raw := range c.ExtraArgs {
		arg := strings.TrimSpace(raw)
		if arg == "" {
			validationErrs = append(validationErrs, fmt.Errorf("cpu_mining.extra_args[%d] cannot be empty", i))
			continue
		}

		c.ExtraArgs[i] = arg
		if reservedFlag, reserved := classifyReservedXMRigFlag(arg); reserved {
			validationErrs = append(validationErrs, fmt.Errorf("cpu_mining.extra_args[%d] uses reserved xmrig flag %q managed by xfo-miner", i, reservedFlag))
		}
	}

	if len(validationErrs) > 0 {
		return errors.Join(validationErrs...)
	}

	return nil
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
	resolvedPath, err := filepath.Abs(filepath.Clean(path))
	if err != nil {
		return nil, fmt.Errorf("resolve config path: %w", err)
	}

	raw, err := os.ReadFile(resolvedPath)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}

	var cfg Config
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&cfg); err != nil {
		return nil, annotateConfigDecodeError(err)
	}

	if err := cfg.applyDefaults(); err != nil {
		return nil, err
	}
	cfg.normalizeXMRigLogPath(filepath.Dir(resolvedPath))
	cfg.normalizeExecutablePaths(filepath.Dir(resolvedPath))
	if err := cfg.ensureStableIdentity(resolvedPath); err != nil {
		return nil, err
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	return &cfg, nil
}

func annotateConfigDecodeError(err error) error {
	if err == nil {
		return nil
	}

	message := err.Error()
	if strings.Contains(message, "in string escape code") {
		return fmt.Errorf("decode config: %w (hint: config.json is strict JSON; on Windows, write paths with forward slashes like ./xmrig.exe or escaped backslashes like .\\\\xmrig.exe)", err)
	}

	return fmt.Errorf("decode config: %w", err)
}

func (c *Config) normalizeXMRigLogPath(configDir string) {
	if !c.CPUMining.Enabled {
		c.CPUMining.XMRigLogPath = strings.TrimSpace(c.CPUMining.XMRigLogPath)
		return
	}

	logPath := strings.TrimSpace(c.CPUMining.XMRigLogPath)
	if logPath == "" {
		logPath = defaultXMRigLogPath
	}
	if !filepath.IsAbs(logPath) {
		logPath = filepath.Join(configDir, logPath)
	}
	c.CPUMining.XMRigLogPath = filepath.Clean(logPath)
}

func (c *Config) normalizeExecutablePaths(configDir string) {
	if executablePathNormalizeGOOS != "windows" {
		return
	}

	c.HashcatPath = normalizeExecutablePathFromConfigDir(configDir, c.HashcatPath)
	c.CPUMining.XMRigPath = normalizeExecutablePathFromConfigDir(configDir, c.CPUMining.XMRigPath)
	c.IdleBehavior.Command = normalizeExecutablePathFromConfigDir(configDir, c.IdleBehavior.Command)
}

func normalizeExecutablePathFromConfigDir(configDir string, raw string) string {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return ""
	}

	if filepath.IsAbs(trimmed) {
		return trimmed
	}

	candidate := filepath.Clean(filepath.Join(configDir, trimmed))
	if _, err := os.Stat(candidate); err == nil {
		return candidate
	}

	return trimmed
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
	if strings.TrimSpace(c.HostPlatformSource) == "" {
		c.HostPlatformSource = strings.TrimSpace(state.HostPlatformSource)
	}

	if strings.TrimSpace(c.PersistentMinerID) == "" {
		id, err := newRandomID()
		if err != nil {
			return fmt.Errorf("generate persistent_miner_id: %w", err)
		}
		c.PersistentMinerID = id
	}
	if strings.TrimSpace(c.HostPlatformID) == "" {
		c.HostPlatformID, c.HostPlatformSource = detectHostPlatformID()
	}

	if strings.TrimSpace(state.OldWorkerName) == "" {
		state.OldWorkerName = strings.TrimSpace(c.WorkerName)
	}

	if strings.TrimSpace(c.HostPlatformID) == "" || c.HostPlatformSource == "hostname_hash_degraded" {
		c.IdentityMode = "legacy_host"
	} else {
		c.IdentityMode = "stable"
	}

	state.HostPlatformID = c.HostPlatformID
	state.HostPlatformSource = c.HostPlatformSource
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

func detectHostPlatformID() (string, string) {
	if hostPlatformGOOS == "linux" {
		if id := detectLinuxMachineID(); id != "" {
			return id, "linux_machine_id"
		}
	}

	if hostPlatformGOOS == "windows" {
		if id := detectWindowsMachineGUID(); id != "" {
			return id, "windows_machine_guid"
		}
	}

	if hostPlatformGOOS == "darwin" {
		if id := detectMacPlatformUUID(); id != "" {
			return id, "mac_platform_uuid"
		}
	}

	hostname, err := os.Hostname()
	if err == nil {
		hostname = strings.TrimSpace(strings.ToLower(hostname))
		if hostname != "" {
			return hostStableHash(hostname), "hostname_hash_degraded"
		}
	}

	return "", ""
}

func detectLinuxMachineID() string {
	candidates := []string{"/etc/machine-id", "/var/lib/dbus/machine-id"}
	for _, p := range candidates {
		raw, err := os.ReadFile(p)
		if err != nil {
			continue
		}
		value := strings.ToLower(strings.TrimSpace(string(raw)))
		if value != "" {
			return hostStableHash(value)
		}
	}
	return ""
}

func detectWindowsMachineGUID() string {
	out, err := runHostProbeCommand("reg", "query", `HKLM\SOFTWARE\Microsoft\Cryptography`, "/v", "MachineGuid")
	if err != nil {
		return ""
	}
	lines := strings.Split(out, "\n")
	for _, line := range lines {
		if !strings.Contains(strings.ToLower(line), "machineguid") {
			continue
		}
		parts := strings.Fields(line)
		if len(parts) == 0 {
			continue
		}
		value := strings.TrimSpace(parts[len(parts)-1])
		if value != "" {
			return hostStableHash(value)
		}
	}
	return ""
}

func detectMacPlatformUUID() string {
	out, err := runHostProbeCommand("ioreg", "-rd1", "-c", "IOPlatformExpertDevice")
	if err == nil {
		lines := strings.Split(out, "\n")
		for _, line := range lines {
			if !strings.Contains(line, "IOPlatformUUID") {
				continue
			}
			if idx := strings.Index(line, "="); idx >= 0 {
				value := strings.TrimSpace(strings.Trim(line[idx+1:], `"`))
				if value != "" {
					return hostStableHash(value)
				}
			}
		}
	}

	out, err = runHostProbeCommand("system_profiler", "SPHardwareDataType")
	if err != nil {
		return ""
	}
	lines := strings.Split(out, "\n")
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if !strings.Contains(strings.ToLower(trimmed), "hardware uuid") {
			continue
		}
		if idx := strings.Index(trimmed, ":"); idx >= 0 {
			value := strings.TrimSpace(trimmed[idx+1:])
			if value != "" {
				return hostStableHash(value)
			}
		}
	}
	return ""
}

func runHostProbeCommand(name string, args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cmd := hostPlatformExecCommandContext(ctx, name, args...)
	out, err := cmd.Output()
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(out)), nil
}

func hostStableHash(value string) string {
	normalized := strings.ToLower(strings.TrimSpace(value))
	if normalized == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(normalized))
	return hex.EncodeToString(sum[:16])
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
	if c.IdleBehavior.Enabled && c.IdleBehavior.RestartCooldownSec == 0 {
		c.IdleBehavior.RestartCooldownSec = 120
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
	if c.IdleBehavior.RestartCooldownSec < 0 {
		validationErrs = append(validationErrs, errors.New("idle_behavior.restart_cooldown_sec cannot be negative"))
	}
	if c.IdleBehavior.Enabled && strings.TrimSpace(c.IdleBehavior.Command) == "" {
		validationErrs = append(validationErrs, errors.New("idle_behavior.command is required when idle_behavior.enabled is true"))
	}
	if c.IdleBehavior.Enabled && validationGOOS == "windows" {
		validationErrs = append(validationErrs, errors.New("idle_behavior.enabled is not supported on Windows; disable idle_behavior or run the idle miner outside xfo-miner"))
	}
	for idx, arg := range c.IdleBehavior.ArgsArray {
		if strings.TrimSpace(arg) == "" {
			validationErrs = append(validationErrs, fmt.Errorf("idle_behavior.args_array[%d] cannot be empty", idx))
		}
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
		if err := c.CPUMining.ValidateExtraArgs(); err != nil {
			validationErrs = append(validationErrs, err)
		}
	}

	if len(validationErrs) > 0 {
		return errors.Join(validationErrs...)
	}
	return nil
}
