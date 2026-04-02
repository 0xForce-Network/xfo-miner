package pool

import "encoding/json"

const (
	PoolStatusAwaitingGenesis = "AWAITING_GENESIS"
	PoolStatusUnarmed         = "UNARMED"
	PoolStatusArmed           = "ARMED"
)

type CapabilitiesData struct {
	HasGPU         bool    `json:"has_gpu"`
	GPUCount       int     `json:"gpu_count"`
	HasHashcat     bool    `json:"has_hashcat"`
	HashcatVersion string  `json:"hashcat_version"`
	HasXMRig       bool    `json:"has_xmrig"`
	XMRigVersion   string  `json:"xmrig_version"`
	HasDocker      bool    `json:"has_docker"`
	AIReady        bool    `json:"ai_ready"`
	BenchmarkKHs   float64 `json:"benchmark_khs"`
	RunMode        string  `json:"run_mode"`
}

// Miner -> Pool
type LoginMessage struct {
	Type          string            `json:"type"`
	NodeID        string            `json:"node_id"`
	WalletAddress string            `json:"wallet_address"`
	WorkerName    string            `json:"worker_name"`
	Version       string            `json:"version"`
	OS            string            `json:"os"`
	Capabilities  *CapabilitiesData `json:"capabilities"`
}

type HeartbeatMessage struct {
	Type string `json:"type"`
}

type ProgressMessage struct {
	Type        string  `json:"type"`
	JobID       string  `json:"job_id"`
	ParentJobID string  `json:"parent_job_id,omitempty"`
	Current     int64   `json:"current"`
	Total       int64   `json:"total"`
	Percent     float64 `json:"percent"`
}

type ResultMessage struct {
	Type        string `json:"type"`
	JobID       string `json:"job_id"`
	ParentJobID string `json:"parent_job_id,omitempty"`
	Status      string `json:"status"`
	Data        string `json:"data"`
}

type ContainerReadyMessage struct {
	Type  string `json:"type"`
	JobID string `json:"job_id"`
	URL   string `json:"url"`
}

type GPUDeviceTelemetry struct {
	DeviceID        string  `json:"device_id"`
	GPUModel        string  `json:"gpu_model"`
	VRAMGB          float64 `json:"vram_gb"`
	Status          string  `json:"status"`
	ReputationScore float64 `json:"reputation_score"`
	PCIBusID        string  `json:"pci_bus_id"`
	Temperature     int     `json:"temperature_c,omitempty"`
	Utilization     int     `json:"utilization_pct,omitempty"`
}

type TelemetryL1Message struct {
	Type      string  `json:"type"`
	Hashrate  float64 `json:"hashrate_khs"`
	Algorithm string  `json:"algorithm"`
	Threads   int     `json:"threads"`
}

type TelemetryL2Message struct {
	Type    string               `json:"type"`
	Devices []GPUDeviceTelemetry `json:"devices"`
	JobID   string               `json:"job_id,omitempty"`
}

// Pool -> Miner
type ServerMessage struct {
	Type string          `json:"type"`
	Raw  json.RawMessage `json:"-"`
}

type JobGPUMessage struct {
	Type        string `json:"type"`
	JobID       string `json:"job_id"`
	HashMode    int    `json:"hash_mode"`
	Target      string `json:"target"`
	Skip        int64  `json:"skip"`
	Limit       int64  `json:"limit"`
	ParentJobID string `json:"parent_job_id,omitempty"`
	ChunkIndex  int    `json:"chunk_index,omitempty"`
}

type JobContainerMessage struct {
	Type       string `json:"type"`
	JobID      string `json:"job_id"`
	Image      string `json:"image"`
	TargetPort int    `json:"target_port"`
}

type PoolStatusMessage struct {
	Type   string `json:"type"`
	Status string `json:"status"`
}

type LoginAckMessage struct {
	Type   string `json:"type"`
	Status string `json:"status"`
}

type OTAUpdateMessage struct {
	Type          string   `json:"type"`
	LatestVersion string   `json:"latest_version"`
	DownloadURLs  []string `json:"download_urls"`
	Checksum      string   `json:"checksum"`
}
