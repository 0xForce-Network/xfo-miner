package scheduler

import (
	"context"
	"encoding/json"
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

type HashcatProbeResult struct {
	Status       string
	ReasonCode   string
	ErrorSummary string
	Version      string
}

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
	stderrEvidence := newBoundedLineBuffer(maxHashcatEvidenceBytes)
	if proc.Stderr != nil {
		stderrWg.Add(1)
		go func() {
			defer stderrWg.Done()
			scanErr := process.ScanLines(proc.Stderr, func(line string) {
				stderrLineCount++
				stderrEvidence.Add(line)
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
	stdoutEvidence := newBoundedLineBuffer(maxHashcatEvidenceBytes)

	if proc.Stdout != nil {
		r.logger.Info("[hashcat] starting stdout scan", "job_id", job.JobID)
		stdoutScanErr := process.ScanLines(proc.Stdout, func(line string) {
			stdoutLineCount++
			stdoutEvidence.Add(line)
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
		} else if reasonCode, ok := classifyHashcatUnsupported(stdoutEvidence.String() + "\n" + stderrEvidence.String()); ok {
			result.Status = "hashcat_unsupported"
			payload := pool.HashcatUnsupportedData{
				CapabilityFingerprint: strings.TrimSpace(job.CapabilityFingerprint),
				ReasonCode:            reasonCode,
				ErrorSummary:          sanitizeHashcatEvidenceSummary(stdoutEvidence.String() + "\n" + stderrEvidence.String()),
			}
			if data, marshalErr := json.Marshal(payload); marshalErr == nil {
				result.Data = string(data)
			} else {
				result.Data = reasonCode
			}
		}
		r.logger.Info("[hashcat] calling onResult", "job_id", job.JobID, "status", result.Status, "data", result.Data)
		onResult(result)
		r.logger.Info("[hashcat] onResult returned", "job_id", job.JobID)
	}

	return nil
}

func (r *HashcatRunner) Probe(ctx context.Context, probe *pool.HashcatCapabilityProbeMessage) (*HashcatProbeResult, error) {
	if probe == nil {
		return nil, fmt.Errorf("hashcat capability probe is nil")
	}
	probeID := strings.TrimSpace(probe.ProbeID)
	if probeID == "" {
		return nil, fmt.Errorf("missing probe_id")
	}
	timeout := time.Duration(probe.TimeoutMS) * time.Millisecond
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	if timeout > 30*time.Second {
		timeout = 30 * time.Second
	}

	probeCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	targetFile, err := os.CreateTemp("", fmt.Sprintf("xfo_hashcat_probe_%s_*.hash", keyspaceJobIDSanitizer.ReplaceAllString(probeID, "_")))
	if err != nil {
		return &HashcatProbeResult{Status: "error", ReasonCode: "probe_tempfile_failed", ErrorSummary: sanitizeHashcatEvidenceSummary(err.Error()), Version: r.hashcatVersion()}, nil
	}
	targetPath := targetFile.Name()
	if _, err := targetFile.WriteString(strings.TrimSpace(probe.ProbePayload.TargetSample) + "\n"); err != nil {
		_ = targetFile.Close()
		_ = os.Remove(targetPath)
		return &HashcatProbeResult{Status: "error", ReasonCode: "probe_tempfile_failed", ErrorSummary: sanitizeHashcatEvidenceSummary(err.Error()), Version: r.hashcatVersion()}, nil
	}
	if err := targetFile.Close(); err != nil {
		_ = os.Remove(targetPath)
		return &HashcatProbeResult{Status: "error", ReasonCode: "probe_tempfile_failed", ErrorSummary: sanitizeHashcatEvidenceSummary(err.Error()), Version: r.hashcatVersion()}, nil
	}
	defer func() {
		_ = os.Remove(targetPath)
	}()

	job := &pool.JobGPUMessage{
		Type:                  "job_gpu",
		JobID:                 "probe_" + probeID,
		CapabilityFingerprint: strings.TrimSpace(probe.CapabilityFingerprint),
		HashMode:              probe.JobShape.HashMode,
		Target:                targetPath,
		Dictionary:            probe.JobShape.Dictionary,
		KeyspaceContract:      probe.JobShape.KeyspaceContract,
		Skip:                  0,
		Limit:                 1,
	}
	if probe.JobShape.AttackMode != nil && len(job.KeyspaceContract) == 0 {
		job.KeyspaceContract = json.RawMessage(fmt.Sprintf(`{"type":"probe_args","attack_mode":%d}`, *probe.JobShape.AttackMode))
	}

	keyspace, err := materializeProbeKeyspaceContract(job, probe)
	if err != nil {
		return &HashcatProbeResult{Status: "error", ReasonCode: "probe_materialization_failed", ErrorSummary: sanitizeHashcatEvidenceSummary(err.Error()), Version: r.hashcatVersion()}, nil
	}
	for _, path := range keyspace.CleanupPaths {
		cleanupPath := strings.TrimSpace(path)
		if cleanupPath == "" {
			continue
		}
		defer func(targetPath string) {
			_ = os.Remove(targetPath)
		}(cleanupPath)
	}

	args := []string{"-m", strconv.Itoa(job.HashMode)}
	if keyspace.AttackMode != nil {
		args = append(args, "-a", strconv.Itoa(*keyspace.AttackMode))
	}
	args = append(args, job.Target)
	args = append(args, keyspace.Inputs...)
	args = append(args, "--potfile-disable", "--runtime", "1", "--status", "--status-timer=1")

	procName := "hashcat_probe_" + keyspaceJobIDSanitizer.ReplaceAllString(probeID, "_")
	proc, err := r.procManager.StartRaw(probeCtx, procName, r.hashcatPath, args)
	if err != nil {
		return &HashcatProbeResult{Status: "error", ReasonCode: "hashcat_probe_spawn_failed", ErrorSummary: sanitizeHashcatEvidenceSummary(err.Error()), Version: r.hashcatVersion()}, nil
	}

	evidence := newBoundedLineBuffer(maxHashcatEvidenceBytes)
	var wg sync.WaitGroup
	if proc.Stdout != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = process.ScanLines(proc.Stdout, func(line string) { evidence.Add(line) })
		}()
	}
	if proc.Stderr != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = process.ScanLines(proc.Stderr, func(line string) { evidence.Add(line) })
		}()
	}

	waitErr := proc.Wait()
	wg.Wait()
	_ = r.procManager.Stop(context.Background(), procName, time.Second)
	if probeCtx.Err() == context.DeadlineExceeded {
		return &HashcatProbeResult{Status: "error", ReasonCode: "hashcat_probe_timeout", ErrorSummary: sanitizeHashcatEvidenceSummary(evidence.String()), Version: r.hashcatVersion()}, nil
	}
	if reasonCode, ok := classifyHashcatUnsupported(evidence.String()); ok {
		return &HashcatProbeResult{Status: "unsupported", ReasonCode: reasonCode, ErrorSummary: sanitizeHashcatEvidenceSummary(evidence.String()), Version: r.hashcatVersion()}, nil
	}
	if waitErr != nil {
		if isHashcatProbeCompletedWithoutCrack(evidence.String()) {
			return &HashcatProbeResult{Status: "supported", ReasonCode: "ok", Version: r.hashcatVersion()}, nil
		}
		summary := evidence.String()
		if strings.TrimSpace(summary) == "" {
			summary = waitErr.Error()
		}
		return &HashcatProbeResult{Status: "error", ReasonCode: "hashcat_probe_failed", ErrorSummary: sanitizeHashcatEvidenceSummary(summary), Version: r.hashcatVersion()}, nil
	}
	return &HashcatProbeResult{Status: "supported", ReasonCode: "ok", Version: r.hashcatVersion()}, nil
}

func (r *HashcatRunner) hashcatVersion() string {
	return ""
}

func materializeProbeKeyspaceContract(job *pool.JobGPUMessage, probe *pool.HashcatCapabilityProbeMessage) (*materializedKeyspace, error) {
	if job != nil && probe != nil && isProbeDictionarySlice(job.KeyspaceContract) && job.Dictionary != nil && strings.TrimSpace(job.Dictionary.RuntimePath) == "" {
		wordlistPath, err := writeCandidateWordlist(job.JobID, []string{"xfo-probe-candidate"})
		if err != nil {
			return nil, err
		}
		attackMode := 0
		if probe.JobShape.AttackMode != nil {
			attackMode = *probe.JobShape.AttackMode
		}
		return &materializedKeyspace{
			AttackMode:   &attackMode,
			Inputs:       []string{wordlistPath},
			Skip:         0,
			Limit:        1,
			CleanupPaths: []string{wordlistPath},
		}, nil
	}
	keyspace, err := materializeKeyspaceContract(job.JobID, job.Dictionary, job.KeyspaceContract, 0, 1)
	if err != nil {
		return nil, err
	}
	if keyspace.AttackMode == nil && probe != nil && probe.JobShape.AttackMode != nil {
		attackMode := *probe.JobShape.AttackMode
		keyspace.AttackMode = &attackMode
	}
	return keyspace, nil
}

func isProbeDictionarySlice(raw json.RawMessage) bool {
	if len(raw) == 0 {
		return false
	}
	var contract keyspaceContract
	if err := json.Unmarshal(raw, &contract); err != nil {
		return false
	}
	return strings.TrimSpace(contract.Type) == "dictionary_slice"
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
