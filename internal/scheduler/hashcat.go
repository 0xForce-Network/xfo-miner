package scheduler

import (
	"context"
	"fmt"
	"log/slog"
	"regexp"
	"strconv"
	"strings"
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
}

func NewHashcatRunner(procManager process.Manager, logger *slog.Logger) *HashcatRunner {
	if logger == nil {
		logger = slog.Default()
	}
	return &HashcatRunner{procManager: procManager, logger: logger}
}

func (r *HashcatRunner) Run(ctx context.Context, job *pool.JobGPUMessage, onProgress func(*pool.ProgressMessage), onResult func(*pool.ResultMessage)) error {
	if job == nil {
		return fmt.Errorf("job_gpu is nil")
	}

	args := []string{
		"-m", strconv.Itoa(job.HashMode),
		job.Target,
		"--skip", strconv.FormatInt(job.Skip, 10),
		"--limit", strconv.FormatInt(job.Limit, 10),
		"--status",
		"--status-timer=5",
	}

	procName := "hashcat_" + job.JobID
	proc, err := r.procManager.Start(ctx, procName, "hashcat", args)
	if err != nil {
		return fmt.Errorf("start hashcat: %w", err)
	}

	cracked := false
	plaintext := ""

	if proc.Stdout != nil {
		_ = process.ScanLines(proc.Stdout, func(line string) {
			if msg, ok := parseProgressLine(line, job.JobID); ok && onProgress != nil {
				onProgress(msg)
			}
			if crackedRegex.MatchString(line) {
				cracked = true
			}
			if matches := plainRegex.FindStringSubmatch(line); len(matches) == 2 {
				plaintext = strings.TrimSpace(matches[1])
			}
		})
	}

	_ = proc.Wait()
	_ = r.procManager.Stop(context.Background(), procName, 1*time.Second)

	if onResult != nil {
		result := &pool.ResultMessage{Type: "result", JobID: job.JobID, Status: "exhausted"}
		if cracked {
			result.Status = "cracked"
			result.Data = plaintext
		}
		onResult(result)
	}

	return nil
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
