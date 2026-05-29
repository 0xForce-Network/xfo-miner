package telemetry

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"testing"

	"github.com/0xforce/xfo-miner/internal/pool"
)

type mockTelemetryPoolClient struct {
	l1 *pool.TelemetryL1Message
	l2 *pool.TelemetryL2Message
}

func (m *mockTelemetryPoolClient) Connect(_ context.Context, _ string) error  { return nil }
func (m *mockTelemetryPoolClient) Close() error                               { return nil }
func (m *mockTelemetryPoolClient) SendLogin(_ *pool.LoginMessage) error       { return nil }
func (m *mockTelemetryPoolClient) SendHeartbeat() error                       { return nil }
func (m *mockTelemetryPoolClient) SendProgress(_ *pool.ProgressMessage) error { return nil }
func (m *mockTelemetryPoolClient) SendResult(_ *pool.ResultMessage) error     { return nil }
func (m *mockTelemetryPoolClient) SendProbeResult(_ *pool.ProbeResultMessage) error {
	return nil
}
func (m *mockTelemetryPoolClient) SendHashcatCapabilityProbeResult(_ *pool.HashcatCapabilityProbeResultMessage) error {
	return nil
}
func (m *mockTelemetryPoolClient) SendContainerReady(_ *pool.ContainerReadyMessage) error {
	return nil
}
func (m *mockTelemetryPoolClient) SendTelemetryL1(msg *pool.TelemetryL1Message) error {
	if msg != nil {
		copy := *msg
		m.l1 = &copy
	}
	return nil
}
func (m *mockTelemetryPoolClient) SendTelemetryL2(msg *pool.TelemetryL2Message) error {
	if msg != nil {
		payload, _ := json.Marshal(msg)
		var copy pool.TelemetryL2Message
		_ = json.Unmarshal(payload, &copy)
		m.l2 = &copy
	}
	return nil
}
func (m *mockTelemetryPoolClient) SendGPUDiagnosticReport(_ *pool.GPUDiagnosticReportMessage) error {
	return nil
}
func (m *mockTelemetryPoolClient) OnMessage(_ func(string, json.RawMessage)) {}
func (m *mockTelemetryPoolClient) OnReconnect(_ func())                      {}

func TestReporterReportsL1AndL2(t *testing.T) {
	t.Parallel()

	pc := &mockTelemetryPoolClient{}
	r := NewReporter("worker-1", 0, pc, slog.New(slog.NewTextHandler(io.Discard, nil)))

	err := r.ReportL1(context.Background(), L1Report{WorkerID: "worker-1", Hashrate: 123.4, Algorithm: "randomx", Threads: 8})
	if err != nil {
		t.Fatalf("ReportL1() error = %v", err)
	}
	if pc.l1 == nil || pc.l1.Hashrate != 123.4 || pc.l1.Type != "telemetry_l1" {
		t.Fatalf("unexpected l1 payload: %+v", pc.l1)
	}

	err = r.ReportL2(context.Background(), L2Report{WorkerID: "worker-1", Devices: []GPUDevice{{DeviceID: "0", GPUModel: "RTX", VRAMGB: 24}}})
	if err != nil {
		t.Fatalf("ReportL2() error = %v", err)
	}
	if pc.l2 == nil || pc.l2.Type != "telemetry_l2" || len(pc.l2.Devices) != 1 || pc.l2.Devices[0].DeviceID != "0" {
		t.Fatalf("unexpected l2 payload: %+v", pc.l2)
	}
}
