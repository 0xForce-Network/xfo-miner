package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"os/signal"
	"syscall"
	"time"

	"github.com/0xforce/xfo-miner/internal/config"
	"github.com/0xforce/xfo-miner/internal/env"
	"github.com/0xforce/xfo-miner/internal/pool"
	"github.com/0xforce/xfo-miner/internal/process"
	"github.com/0xforce/xfo-miner/internal/scheduler"
	"github.com/0xforce/xfo-miner/internal/updater"
)

const version = "0.1.0"

func main() {
	configPath := flag.String("config", "./config.json", "path to config.json")
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	printBanner(logger)
	if err := updater.CleanupOldBinary(); err != nil {
		logger.Warn("failed to cleanup old OTA binary artifact", "error", err)
	}

	cfg, err := config.LoadConfig(*configPath)
	if err != nil {
		logger.Error("failed to load config", "path", *configPath, "error", err)
		os.Exit(1)
	}

	logger.Info("config loaded",
		"node_id", cfg.NodeID,
		"worker_name", cfg.WorkerName,
		"pool_url", cfg.PoolURL,
		"max_cpu_threads", cfg.MaxCPUThreads,
		"idle_enabled", cfg.IdleBehavior.Enabled,
	)

	capabilities, capErr := env.ProbeAll(context.Background())
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

	poolClient := pool.NewWSSClient(logger)
	s := scheduler.New(cfg, version, capabilities, process.NewRealManager(logger), poolClient, logger)
	if err := s.Run(ctx); err != nil {
		logger.Error("scheduler exited with error", "error", err)
		os.Exit(1)
	}

	logger.Info("scheduler stopped gracefully")
}

func printBanner(logger *slog.Logger) {
	logger.Info(fmt.Sprintf("xfo-miner v%s", version))
}
