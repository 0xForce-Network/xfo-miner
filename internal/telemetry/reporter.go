package telemetry

import (
	"context"
	"log/slog"
	"runtime"
	"time"

	"github.com/0xforce/xfo-miner/internal/pool"
)

type L1Report struct {
	WorkerID  string  `json:"worker_id"`
	Hashrate  float64 `json:"hashrate_khs"`
	Algorithm string  `json:"algorithm"`
	Threads   int     `json:"threads"`
	Timestamp int64   `json:"timestamp"`
}

type L2Report struct {
	WorkerID  string      `json:"worker_id"`
	Devices   []GPUDevice `json:"devices"`
	JobID     string      `json:"job_id,omitempty"`
	Timestamp int64       `json:"timestamp"`
}

type Reporter struct {
	workerID string
	logger   *slog.Logger
	interval time.Duration
	pool     pool.Client
}

func NewReporter(workerID string, interval time.Duration, poolClient pool.Client, logger *slog.Logger) *Reporter {
	if logger == nil {
		logger = slog.Default()
	}
	if interval <= 0 {
		interval = 30 * time.Second
	}
	return &Reporter{workerID: workerID, interval: interval, pool: poolClient, logger: logger}
}

func (r *Reporter) ReportL1(_ context.Context, report L1Report) error {
	return r.pool.SendTelemetryL1(&pool.TelemetryL1Message{
		Type:      "telemetry_l1",
		Hashrate:  report.Hashrate,
		Algorithm: report.Algorithm,
		Threads:   report.Threads,
	})
}

func (r *Reporter) ReportL2(_ context.Context, report L2Report) error {
	devices := make([]pool.GPUDeviceTelemetry, 0, len(report.Devices))
	for _, d := range report.Devices {
		devices = append(devices, pool.GPUDeviceTelemetry{
			DeviceID:        d.DeviceID,
			GPUModel:        d.GPUModel,
			VRAMGB:          d.VRAMGB,
			Status:          d.Status,
			ReputationScore: d.ReputationScore,
			PCIBusID:        d.PCIBusID,
			Temperature:     d.Temperature,
			Utilization:     d.Utilization,
		})
	}
	return r.pool.SendTelemetryL2(&pool.TelemetryL2Message{Type: "telemetry_l2", Devices: devices, JobID: report.JobID})
}

func (r *Reporter) RunL1Loop(ctx context.Context) {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_ = r.ReportL1(ctx, L1Report{
				WorkerID:  r.workerID,
				Hashrate:  0,
				Algorithm: "randomx",
				Threads:   runtime.NumCPU(),
				Timestamp: time.Now().Unix(),
			})
		}
	}
}

func (r *Reporter) RunL2Loop(ctx context.Context) {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			devices, err := ScanGPUs()
			if err != nil {
				r.logger.Warn("telemetry gpu scan failed", "error", err)
				continue
			}
			_ = r.ReportL2(ctx, L2Report{WorkerID: r.workerID, Devices: devices, Timestamp: time.Now().Unix()})
		}
	}
}
