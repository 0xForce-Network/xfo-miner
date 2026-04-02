package env

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
)

var speedLineRegex = regexp.MustCompile(`(?m)^Speed\.#\d+.*?:\s*([0-9]+(?:\.[0-9]+)?)\s*([kMGT]?H/s)\b`)
var machineReadableRegex = regexp.MustCompile(`(?m)^\d+:\d+:[^\n]+$`)

type BenchmarkResult struct {
	HashMode int
	SpeedKHs float64
	GPUName  string
}

func RunHashcatBenchmark(ctx context.Context, hashcatPath string) (*BenchmarkResult, error) {
	if strings.TrimSpace(hashcatPath) == "" {
		hashcatPath = "hashcat"
	}

	output, err := runCommand(ctx, 60*time.Second, hashcatPath, "-b", "-m", "22000", "--machine-readable")
	if err != nil {
		return nil, err
	}

	result, parseErr := parseBenchmarkOutput(output)
	if parseErr != nil {
		return nil, parseErr
	}
	result.HashMode = 22000

	return result, nil
}

func parseBenchmarkOutput(output string) (*BenchmarkResult, error) {
	matches := speedLineRegex.FindAllStringSubmatch(output, -1)
	if len(matches) > 0 {
		totalKHs := 0.0
		for _, match := range matches {
			value, err := strconv.ParseFloat(match[1], 64)
			if err != nil {
				return nil, fmt.Errorf("parse benchmark speed: %w", err)
			}

			scaled, err := toKHs(value, match[2])
			if err != nil {
				return nil, err
			}
			totalKHs += scaled
		}

		gpuName := extractGPUName(output)

		return &BenchmarkResult{
			SpeedKHs: totalKHs,
			GPUName:  gpuName,
		}, nil
	}

	machineLines := machineReadableRegex.FindAllString(strings.TrimSpace(output), -1)
	if len(machineLines) == 0 {
		return nil, fmt.Errorf("hashcat benchmark output missing recognizable speed format")
	}

	totalHps := 0.0
	for _, line := range machineLines {
		fields := strings.Split(line, ":")
		if len(fields) < 6 {
			continue
		}
		hps, err := strconv.ParseFloat(strings.TrimSpace(fields[5]), 64)
		if err != nil {
			continue
		}
		totalHps += hps
	}

	if totalHps <= 0 {
		return nil, fmt.Errorf("hashcat benchmark output has no positive speed value")
	}

	return &BenchmarkResult{
		SpeedKHs: totalHps / 1000.0,
		GPUName:  extractGPUName(output),
	}, nil
}

func toKHs(value float64, unit string) (float64, error) {
	switch unit {
	case "H/s":
		return value / 1000.0, nil
	case "kH/s":
		return value, nil
	case "MH/s":
		return value * 1000.0, nil
	case "GH/s":
		return value * 1000_000.0, nil
	case "TH/s":
		return value * 1000_000_000.0, nil
	default:
		return 0, fmt.Errorf("unsupported benchmark unit: %s", unit)
	}
}

func extractGPUName(output string) string {
	lines := strings.Split(output, "\n")
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "Device #") {
			return trimmed
		}
	}
	return ""
}
