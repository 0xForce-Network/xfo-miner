package scheduler

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/0xforce/xfo-miner/internal/pool"
	"github.com/0xforce/xfo-miner/internal/process"
)

var (
	progressRegex = regexp.MustCompile(`Progress:(\d+)/(\d+)\s+\(([0-9.]+)%\)`)
	crackedRegex  = regexp.MustCompile(`(?i)^Status\.*:\s*Cracked`)
	plainRegex    = regexp.MustCompile(`(?i)^Plaintext\.*:\s*(.+)$`)
)

type HashcatRunner struct {
	procManager process.Manager
	logger      *slog.Logger
	hashcatPath string
}

func NewHashcatRunner(procManager process.Manager, hashcatPath string, logger *slog.Logger) *HashcatRunner {
	if logger == nil {
		logger = slog.Default()
	}
	if strings.TrimSpace(hashcatPath) == "" {
		hashcatPath = "hashcat"
	}
	return &HashcatRunner{procManager: procManager, logger: logger, hashcatPath: hashcatPath}
}

func (r *HashcatRunner) Run(ctx context.Context, job *pool.JobGPUMessage, onProgress func(*pool.ProgressMessage), onResult func(*pool.ResultMessage)) error {
	if job == nil {
		return fmt.Errorf("job_gpu is nil")
	}
	r.logger.Info("[hashcat] Run() entered",
		"job_id", job.JobID,
		"target", job.Target,
		"target_url", job.TargetURL,
		"hash_mode", job.HashMode,
		"skip", job.Skip,
		"limit", job.Limit,
		"has_keyspace_contract", len(job.KeyspaceContract) > 0,
	)
	keyspace, err := materializeKeyspaceContract(job.JobID, job.Dictionary, job.KeyspaceContract, job.Skip, job.Limit)
	if err != nil {
		r.logger.Error("[hashcat] materializeKeyspaceContract failed", "job_id", job.JobID, "error", err)
		if errors.Is(err, ErrInvalidKeyspaceContract) || errors.Is(err, ErrUnsupportedKeyspaceContract) || errors.Is(err, ErrCandidateMaterializationFailed) {
			return err
		}
		return fmt.Errorf("%w: %v", ErrCandidateMaterializationFailed, err)
	}
	r.logger.Info("[hashcat] keyspace materialized",
		"job_id", job.JobID,
		"attack_mode", keyspace.AttackMode,
		"inputs", keyspace.Inputs,
		"skip", keyspace.Skip,
		"limit", keyspace.Limit,
		"cleanup_paths", keyspace.CleanupPaths,
	)
	for _, path := range keyspace.CleanupPaths {
		cleanupPath := strings.TrimSpace(path)
		if cleanupPath == "" {
			continue
		}
		defer func(targetPath string) {
			_ = os.Remove(targetPath)
		}(cleanupPath)
	}

	resultFile, err := os.CreateTemp("", fmt.Sprintf("xfo_hashcat_%s_*.result", strings.TrimSpace(job.JobID)))
	if err != nil {
		return fmt.Errorf("create hashcat result file: %w", err)
	}
	resultPath := resultFile.Name()
	if err := resultFile.Close(); err != nil {
		_ = os.Remove(resultPath)
		return fmt.Errorf("close hashcat result file: %w", err)
	}
	defer func() {
		_ = os.Remove(resultPath)
	}()

	args := []string{"-m", strconv.Itoa(job.HashMode)}
	if keyspace.AttackMode != nil {
		args = append(args, "-a", strconv.Itoa(*keyspace.AttackMode))
	}
	args = append(args, job.Target)
	args = append(args, keyspace.Inputs...)
	args = append(args,
		"--outfile", resultPath,
		"--outfile-format", "2",
		"--outfile-autohex-disable",
		"--potfile-disable",
		"--skip", strconv.FormatInt(keyspace.Skip, 10),
		"--limit", strconv.FormatInt(keyspace.Limit, 10),
		"--status",
		"--status-timer=5",
	)

	procName := "hashcat_" + job.JobID
	r.logger.Info("[hashcat] launching process",
		"job_id", job.JobID,
		"proc_name", procName,
		"command", r.hashcatPath,
		"args", strings.Join(args, " "),
		"result_file", resultPath,
		"ctx_err", ctx.Err(),
	)
	proc, err := r.procManager.StartRaw(ctx, procName, r.hashcatPath, args)
	if err != nil {
		r.logger.Error("[hashcat] StartRaw failed", "job_id", job.JobID, "error", err)
		return fmt.Errorf("start hashcat: %w", err)
	}
	r.logger.Info("[hashcat] process started successfully", "job_id", job.JobID, "proc_name", procName)

	var stderrWg sync.WaitGroup
	stderrLineCount := 0
	if proc.Stderr != nil {
		stderrWg.Add(1)
		go func() {
			defer stderrWg.Done()
			scanErr := process.ScanLines(proc.Stderr, func(line string) {
				stderrLineCount++
			})
			r.logger.Info("[hashcat] stderr drain finished",
				"job_id", job.JobID,
				"stderr_lines", stderrLineCount,
				"scan_err", scanErr,
			)
		}()
	} else {
		r.logger.Warn("[hashcat] proc.Stderr is nil — no stderr drain", "job_id", job.JobID)
	}

	cracked := false
	plaintext := ""
	stdoutLineCount := 0

	if proc.Stdout != nil {
		r.logger.Info("[hashcat] starting stdout scan", "job_id", job.JobID)
		stdoutScanErr := process.ScanLines(proc.Stdout, func(line string) {
			stdoutLineCount++
			if msg, ok := parseProgressLine(line, job.JobID); ok && onProgress != nil {
				onProgress(msg)
			}
			if crackedRegex.MatchString(line) {
				cracked = true
				r.logger.Info("[hashcat] STATUS CRACKED detected in stdout", "job_id", job.JobID, "line", line)
			}
			if matches := plainRegex.FindStringSubmatch(line); len(matches) == 2 {
				plaintext = strings.TrimSpace(matches[1])
				r.logger.Info("[hashcat] PLAINTEXT detected in stdout", "job_id", job.JobID, "plaintext", plaintext)
			}
		})
		r.logger.Info("[hashcat] stdout scan finished",
			"job_id", job.JobID,
			"stdout_lines", stdoutLineCount,
			"scan_err", stdoutScanErr,
			"cracked_from_stdout", cracked,
			"plaintext_from_stdout", plaintext,
		)
	} else {
		r.logger.Warn("[hashcat] proc.Stdout is nil — no stdout to scan", "job_id", job.JobID)
	}

	waitErr := proc.Wait()
	r.logger.Info("[hashcat] proc.Wait() returned",
		"job_id", job.JobID,
		"wait_err", waitErr,
		"ctx_err", ctx.Err(),
	)
	stderrWg.Wait()
	r.logger.Info("[hashcat] stderr wg done", "job_id", job.JobID)

	stopErr := r.procManager.Stop(context.Background(), procName, 1*time.Second)
	r.logger.Info("[hashcat] procManager.Stop returned", "job_id", job.JobID, "stop_err", stopErr)

	if resultBytes, readErr := os.ReadFile(resultPath); readErr == nil {
		resultContent := strings.TrimSpace(string(resultBytes))
		r.logger.Info("[hashcat] result file read",
			"job_id", job.JobID,
			"result_file", resultPath,
			"content_length", len(resultContent),
			"content_preview", truncateForLog(resultContent, 500),
		)
		lines := strings.Split(resultContent, "\n")
		for _, line := range lines {
			candidate := strings.TrimSpace(line)
			if candidate == "" {
				continue
			}
			if idx := strings.LastIndex(candidate, ":"); idx >= 0 && idx < len(candidate)-1 {
				candidate = strings.TrimSpace(candidate[idx+1:])
			}
			if candidate == "" {
				continue
			}
			plaintext = candidate
			cracked = true
			break
		}
	} else {
		r.logger.Info("[hashcat] result file read failed",
			"job_id", job.JobID,
			"result_file", resultPath,
			"read_err", readErr,
		)
	}

	r.logger.Info("[hashcat] FINAL RESULT before onResult callback",
		"job_id", job.JobID,
		"cracked", cracked,
		"plaintext", plaintext,
		"stdout_lines", stdoutLineCount,
		"stderr_lines", stderrLineCount,
		"wait_err", waitErr,
		"ctx_err", ctx.Err(),
		"onResult_nil", onResult == nil,
	)

	if onResult != nil {
		result := &pool.ResultMessage{Type: "result", JobID: job.JobID, Status: "exhausted"}
		if cracked {
			result.Status = "cracked"
			result.Data = plaintext
		}
		r.logger.Info("[hashcat] calling onResult", "job_id", job.JobID, "status", result.Status, "data", result.Data)
		onResult(result)
		r.logger.Info("[hashcat] onResult returned", "job_id", job.JobID)
	}

	return nil
}

func truncateForLog(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "...(truncated)"
}

func parseProgressLine(line string, jobID string) (*pool.ProgressMessage, bool) {
	matches := progressRegex.FindStringSubmatch(line)
	if len(matches) != 4 {
		return nil, false
	}

	current, err := strconv.ParseInt(matches[1], 10, 64)
	if err != nil {
		return nil, false
	}
	total, err := strconv.ParseInt(matches[2], 10, 64)
	if err != nil {
		return nil, false
	}
	percent, err := strconv.ParseFloat(matches[3], 64)
	if err != nil {
		return nil, false
	}

	return &pool.ProgressMessage{
		Type:    "progress",
		JobID:   jobID,
		Current: current,
		Total:   total,
		Percent: percent,
	}, true
}
