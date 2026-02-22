package scheduler

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"regexp"
	"time"

	"github.com/0xforce/xfo-miner/internal/pool"
	"github.com/0xforce/xfo-miner/internal/process"
)

var tunnelURLRegex = regexp.MustCompile(`https://[a-z0-9-]+\.trycloudflare\.com`)

type ContainerRunner struct {
	procManager process.Manager
	logger      *slog.Logger
}

func NewContainerRunner(procManager process.Manager, logger *slog.Logger) *ContainerRunner {
	if logger == nil {
		logger = slog.Default()
	}
	return &ContainerRunner{procManager: procManager, logger: logger}
}

func (r *ContainerRunner) Run(ctx context.Context, job *pool.JobContainerMessage) (string, error) {
	if job == nil {
		return "", fmt.Errorf("job_container is nil")
	}

	port, err := allocateLocalPort()
	if err != nil {
		return "", err
	}

	dockerArgs := []string{"run", "-d", "--gpus", "all", "-p", fmt.Sprintf("127.0.0.1:%d:%d", port, job.TargetPort), job.Image}
	procName := "docker_" + job.JobID
	dockerProc, err := r.procManager.Start(ctx, procName, "docker", dockerArgs)
	if err != nil {
		return "", fmt.Errorf("start docker: %w", err)
	}
	_ = dockerProc.Wait()

	tunnelName := "cloudflared_" + job.JobID
	tunnelArgs := []string{"tunnel", "--url", fmt.Sprintf("http://127.0.0.1:%d", port)}
	tunnelProc, err := r.procManager.Start(ctx, tunnelName, "cloudflared", tunnelArgs)
	if err != nil {
		return "", fmt.Errorf("start cloudflared: %w", err)
	}

	tunnelCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	urlCh := make(chan string, 1)
	if tunnelProc.Stdout != nil {
		go func() {
			_ = process.ScanLines(tunnelProc.Stdout, func(line string) {
				if url := tunnelURLRegex.FindString(line); url != "" {
					select {
					case urlCh <- url:
					default:
					}
				}
			})
		}()
	}

	select {
	case url := <-urlCh:
		return url, nil
	case <-tunnelCtx.Done():
		_ = r.procManager.Stop(context.Background(), tunnelName, 1*time.Second)
		return "", fmt.Errorf("cloudflared url timeout: %w", tunnelCtx.Err())
	}
}

func allocateLocalPort() (int, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, fmt.Errorf("allocate port: %w", err)
	}
	defer listener.Close()

	addr, ok := listener.Addr().(*net.TCPAddr)
	if !ok {
		return 0, fmt.Errorf("unexpected listener address type")
	}

	return addr.Port, nil
}
