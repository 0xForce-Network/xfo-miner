package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"syscall"
	"time"

	"github.com/0xforce/xfo-miner/internal/config"
	"github.com/0xforce/xfo-miner/internal/debuglog"
	"github.com/0xforce/xfo-miner/internal/env"
	"github.com/0xforce/xfo-miner/internal/pool"
	"github.com/0xforce/xfo-miner/internal/process"
	"github.com/0xforce/xfo-miner/internal/scheduler"
	"github.com/0xforce/xfo-miner/internal/telemetry"
	"github.com/0xforce/xfo-miner/internal/updater"
)

var version = "1.0.13"
var gitCommit = "dev"
var buildTime = "unknown"

func main() {
	configPath := flag.String("config", "./config.json", "path to config.json")
	debug := flag.Bool("debug", false, "enable debug mode and disable forced OTA")
	debugVerbose := flag.Bool("debug-verbose", false, "enable verbose debug miner diagnostics (requires --debug)")
	insecure := flag.Bool("insecure", false, "skip TLS certificate verification (testnet only)")
	logDir := flag.String("log-dir", "", "directory to write subprocess log files (xmrig, idle miner, etc.)")
	showVersion := flag.Bool("version", false, "print version and exit")

	flag.Usage = func() { printHelp() }
	flag.Parse()

	if *showVersion {
		fmt.Printf("xfo-miner v%s\n", version)
		os.Exit(0)
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	printBanner(logger)
	if err := updater.CleanupOldBinary(); err != nil {
		logger.Warn("failed to cleanup old OTA binary artifact", "error", err)
	}
	resolvedConfigPath, err := filepath.Abs(filepath.Clean(*configPath))
	if err != nil {
		logger.Error("failed to resolve config path", "path", *configPath, "error", err)
		os.Exit(1)
	}
	if *debug {
		debugPath := filepath.Join(filepath.Dir(resolvedConfigPath), "debug.log")
		if err := debuglog.Enable(debugPath, *debugVerbose); err != nil {
			logger.Error("failed to initialize debug log", "path", debugPath, "error", err)
			os.Exit(1)
		}
		defer func() {
			if err := debuglog.Close(); err != nil {
				logger.Warn("failed to close debug log", "error", err)
			}
		}()
	}

	cfg, err := config.LoadConfig(*configPath)
	if err != nil {
		logger.Error("failed to load config", "path", *configPath, "error", err)
		os.Exit(1)
	}

	instanceGuard, err := process.NewInstanceGuard(cfg.IdentityStatePath())
	if err != nil {
		logger.Error("failed to create single-instance guard", "error", err)
		os.Exit(1)
	}
	if err := instanceGuard.Acquire(10 * time.Second); err != nil {
		logger.Error("another xfo-miner instance is already running", "error", err)
		os.Exit(1)
	}
	defer func() {
		if err := instanceGuard.Release(); err != nil {
			logger.Warn("failed to release single-instance guard", "error", err)
		}
	}()

	logger.Info("config loaded",
		"node_id", cfg.NodeID,
		"worker_name", cfg.WorkerName,
		"pool_url", cfg.PoolURL,
		"max_cpu_threads", cfg.MaxCPUThreads,
		"idle_enabled", cfg.IdleBehavior.Enabled,
		"debug", *debug,
	)
	if *debug {
		logger.Warn("debug mode enabled; forced OTA is disabled")
		debuglog.Log("debug_mode_enabled",
			"ota_disabled", true,
			"debug_verbose", *debugVerbose,
			"miner_version", version,
			"git_commit", gitCommit,
			"build_time", buildTime,
			"os", runtime.GOOS,
			"arch", runtime.GOARCH,
			"config_path", resolvedConfigPath,
			"debug_log_path", debuglog.Path(),
			"pool_url", cfg.PoolURL,
			"worker_name", cfg.WorkerName,
			"wallet_suffix", maskWalletSuffix(cfg.WalletAddress),
		)
	}

	capabilities, capErr := env.ProbeAll(context.Background(), cfg.HashcatPath, cfg.CPUMining.XMRigPath)
	if capErr != nil {
		logger.Warn("environment probe completed with warnings", "error", capErr)
	}

	if cfg.CPUMining.Enabled {
		checkCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		cmd := exec.CommandContext(checkCtx, cfg.CPUMining.XMRigPath, "--version")
		if _, err := cmd.CombinedOutput(); err != nil {
			logger.Error("FATAL: xmrig binary not found. Download the complete 0xForce miner package.", "xmrig_path", cfg.CPUMining.XMRigPath, "error", err)
			os.Exit(1)
		}
		if !capabilities.HasXMRig {
			logger.Error("FATAL: xmrig probe failed although cpu_mining.enabled=true", "xmrig_path", cfg.CPUMining.XMRigPath)
			os.Exit(1)
		}
	}

	logger.Info("system capabilities",
		"has_gpu", capabilities.HasGPU,
		"gpu_count", len(capabilities.GPUs),
		"has_hashcat", capabilities.HasHashcat,
		"hashcat_version", capabilities.HashcatVersion,
		"has_xmrig", capabilities.HasXMRig,
		"xmrig_version", capabilities.XMRigVersion,
		"has_docker", capabilities.HasDocker,
		"ai_ready", capabilities.AIReady,
		"benchmark_khs", capabilities.BenchmarkKHs,
		"run_mode", capabilities.RunMode,
	)

	if !capabilities.IsRoot {
		logger.Warn("miner is not running with root/admin privileges; some features may be unavailable")
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	var poolOpts []pool.ClientOption
	if *insecure {
		logger.Warn("TLS certificate verification disabled (--insecure flag)")
		poolOpts = append(poolOpts, pool.WithInsecureSkipVerify())
	}
	var poolClient pool.Client
	if cfg.L2Enabled() {
		poolClient = pool.NewWSSClient(logger, poolOpts...)
		reporter := telemetry.NewReporter(cfg.NodeID, 30*time.Second, poolClient, logger)
		go reporter.RunL1Loop(ctx)
		go reporter.RunL2Loop(ctx)
		logger.Info("L2 pool WebSocket enabled", "pool_url", cfg.PoolURL)
	} else {
		poolClient = pool.NewNoopClient()
		logger.Warn("L2 pool WebSocket DISABLED — running in L1-only mode (no telemetry, no GPU tasks)")
	}

	s := scheduler.New(cfg, version, capabilities, process.NewRealManager(logger, process.WithLogDir(*logDir)), poolClient, logger)
	s.SetDebugMode(*debug)
	s.SetDebugBuildInfo(gitCommit, buildTime)
	s.SetOTAHandoffStartedHook(instanceGuard.Release)
	if err := s.Run(ctx); err != nil {
		logger.Error("scheduler exited with error", "error", err)
		os.Exit(1)
	}

	logger.Info("scheduler stopped gracefully")
}

func printBanner(logger *slog.Logger) {
	logger.Info(fmt.Sprintf("xfo-miner v%s", version))
}

func printHelp() {
	help := `xfo-miner v` + version + ` — 0xForce Network Mining Client

USAGE:
  xfo-miner [flags]

CLI FLAGS:
  --config <path>      Path to config.json (default: ./config.json)
  --debug              Enable debug mode and disable forced OTA
  --debug-verbose      Enable verbose debug miner diagnostics (requires --debug)
  --insecure           Skip TLS certificate verification (testnet only)
  --log-dir <path>     Directory to write subprocess log files (xmrig, idle miner, etc.)
  --version            Print version and exit
  --help               Show this help message

CONFIG FILE PARAMETERS (config.json):

  Top-level:
    node_id              (string)  Unique miner node identifier (optional; auto-generated if empty)
    worker_name          (string)  Human-readable worker/rig name (required)
    pool_url             (string)  Pool WebSocket endpoint, wss:// or ws:// (optional; leave empty for L1-only mode)
    max_cpu_threads      (int)     Max CPU threads for xfo-miner task execution and default xmrig full-mode cap (default: half of system CPUs)

  cpu_mining:
    enabled              (bool)    Enable CPU mining via xmrig (default: false)
    xmrig_path           (string)  Path to xmrig binary (required when enabled)
    xmrig_log_path       (string)  Local xmrig stdout/stderr log path (default: ./logs/xmrig.log relative to config; 3-day retention)
    stratum_url          (string)  Stratum pool URL, e.g. stratum+tcp://host:3333 (required when enabled)
    max_threads          (int)     XMRig threads in full mining mode (default: max_cpu_threads)
    background_threads   (int)     XMRig threads in standby/heartbeat mode (default: 1)
    extra_args           ([]str)   Extra xmrig CLI args, e.g. ["--proxy=127.0.0.1:1080"] (reserved control flags rejected)

  idle_behavior:
    enabled              (bool)    Enable idle-mode fallback mining (default: false; keep disabled unless you have an explicit idle miner command)
    grace_period_sec     (int)     Seconds to wait before entering idle mode (default: 0)
    command              (string)  Idle miner binary path (required when enabled)
    args                 (string)  Arguments passed to idle miner command

EXAMPLES:
  xfo-miner --config /etc/xfo/config.json
  xfo-miner --config ./config.json --debug
  xfo-miner --config ./config.json --debug --debug-verbose
  xfo-miner --config ./config.json --insecure --log-dir /var/log/xfo
  xfo-miner --version

See config.example.json for a full configuration template.
`
	fmt.Print(help)
}

func maskWalletSuffix(wallet string) string {
	trimmed := filepath.Base(wallet)
	if len(trimmed) <= 8 {
		return trimmed
	}
	return trimmed[len(trimmed)-8:]
}
