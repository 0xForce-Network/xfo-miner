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
	Type         string            `json:"type"`
	NodeID       string            `json:"node_id"`
	WorkerName   string            `json:"worker_name"`
	Version      string            `json:"version"`
	OS           string            `json:"os"`
	Capabilities *CapabilitiesData `json:"capabilities"`
}

type HeartbeatMessage struct {
	Type string `json:"type"`
}

type ProgressMessage struct {
	Type    string  `json:"type"`
	JobID   string  `json:"job_id"`
	Current int64   `json:"current"`
	Total   int64   `json:"total"`
	Percent float64 `json:"percent"`
}

type ResultMessage struct {
	Type   string `json:"type"`
	JobID  string `json:"job_id"`
	Status string `json:"status"`
	Data   string `json:"data"`
}

type ContainerReadyMessage struct {
	Type  string `json:"type"`
	JobID string `json:"job_id"`
	URL   string `json:"url"`
}

// Pool -> Miner
type ServerMessage struct {
	Type string          `json:"type"`
	Raw  json.RawMessage `json:"-"`
}

type JobGPUMessage struct {
	Type     string `json:"type"`
	JobID    string `json:"job_id"`
	HashMode int    `json:"hash_mode"`
	Target   string `json:"target"`
	Skip     int64  `json:"skip"`
	Limit    int64  `json:"limit"`
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
