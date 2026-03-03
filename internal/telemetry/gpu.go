package telemetry

import (
	"context"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

var execCommandContext = exec.CommandContext
var lookPath = exec.LookPath

type GPUDevice struct {
	DeviceID        string  `json:"device_id"`
	GPUModel        string  `json:"gpu_model"`
	VRAMGB          float64 `json:"vram_gb"`
	Status          string  `json:"status"`
	ReputationScore float64 `json:"reputation_score"`
	PCIBusID        string  `json:"pci_bus_id"`
	Temperature     int     `json:"temperature_c,omitempty"`
	Utilization     int     `json:"utilization_pct,omitempty"`
}

func ScanGPUs() ([]GPUDevice, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cmd := execCommandContext(ctx, "nvidia-smi", "--query-gpu=index,name,memory.total,pci.bus_id,temperature.gpu,utilization.gpu", "--format=csv,noheader,nounits")
	out, err := cmd.Output()
	if err != nil {
		if _, lookErr := lookPath("nvidia-smi"); lookErr != nil {
			return []GPUDevice{}, nil
		}
		return nil, err
	}

	lines := strings.Split(strings.TrimSpace(string(out)), "\n")
	devices := make([]GPUDevice, 0, len(lines))
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		parts := strings.Split(line, ",")
		if len(parts) < 6 {
			continue
		}

		idx := strings.TrimSpace(parts[0])
		name := strings.TrimSpace(parts[1])
		memMB, _ := strconv.ParseFloat(strings.TrimSpace(parts[2]), 64)
		temp, _ := strconv.Atoi(strings.TrimSpace(parts[4]))
		util, _ := strconv.Atoi(strings.TrimSpace(parts[5]))

		devices = append(devices, GPUDevice{
			DeviceID:        idx,
			GPUModel:        name,
			VRAMGB:          memMB / 1024.0,
			Status:          "idle",
			ReputationScore: 50.0,
			PCIBusID:        strings.TrimSpace(parts[3]),
			Temperature:     temp,
			Utilization:     util,
		})
	}

	return devices, nil
}
