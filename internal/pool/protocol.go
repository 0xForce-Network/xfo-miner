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
	Type                       string            `json:"type"`
	NodeID                     string            `json:"node_id"`
	WalletAddress              string            `json:"wallet_address"`
	WorkerName                 string            `json:"worker_name"`
	LegacyClaim                *LegacyClaim      `json:"legacy_claim,omitempty"`
	HostPlatformID             string            `json:"host_platform_id,omitempty"`
	HostPlatformSource         string            `json:"host_platform_source,omitempty"`
	PersistentMinerID          string            `json:"persistent_miner_id,omitempty"`
	IdentityMode               string            `json:"identity_mode,omitempty"`
	Devices                    []GPUIdentity     `json:"devices,omitempty"`
	LastVerifiedEpochID        string            `json:"last_verified_epoch_id,omitempty"`
	LastVerifiedAt             int64             `json:"last_verified_at,omitempty"`
	VerificationState          string            `json:"verification_state,omitempty"`
	VerificationDeferredReason string            `json:"verification_deferred_reason,omitempty"`
	Version                    string            `json:"version"`
	OS                         string            `json:"os"`
	Capabilities               *CapabilitiesData `json:"capabilities"`
}

type LegacyClaim struct {
	OldWorkerName   string `json:"old_worker_name"`
	MigrationReason string `json:"migration_reason"`
}

type GPUIdentity struct {
	DeviceIndex       int     `json:"device_index"`
	VendorID          string  `json:"vendor_id,omitempty"`
	DeviceID          string  `json:"device_id,omitempty"`
	UUIDSource        string  `json:"uuid_source,omitempty"`
	GPUUUID           string  `json:"gpu_uuid,omitempty"`
	DeviceFingerprint string  `json:"device_fingerprint,omitempty"`
	PCIBusID          string  `json:"pci_bus_id,omitempty"`
	GPUModel          string  `json:"gpu_model,omitempty"`
	VRAMGB            float64 `json:"vram_gb,omitempty"`
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
	ResultKind  string `json:"result_kind,omitempty"`
	Data        string `json:"data"`
}

type HashcatUnsupportedData struct {
	CapabilityFingerprint string `json:"capability_fingerprint,omitempty"`
	ReasonCode            string `json:"reason_code"`
	ErrorSummary          string `json:"error_summary,omitempty"`
}

type ProbeResultMessage struct {
	Type        string `json:"type"`
	ChallengeID string `json:"challenge_id"`
	Status      string `json:"status"`
	Result      string `json:"result,omitempty"`
	ErrorCode   string `json:"error_code,omitempty"`
}

type ContainerReadyMessage struct {
	Type  string `json:"type"`
	JobID string `json:"job_id"`
	URL   string `json:"url"`
}

type GPUDeviceTelemetry struct {
	DeviceID          string  `json:"device_id"`
	DeviceIndex       int     `json:"device_index,omitempty"`
	VendorID          string  `json:"vendor_id,omitempty"`
	UUIDSource        string  `json:"uuid_source,omitempty"`
	GPUUUID           string  `json:"gpu_uuid,omitempty"`
	DeviceFingerprint string  `json:"device_fingerprint,omitempty"`
	GPUModel          string  `json:"gpu_model"`
	VRAMGB            float64 `json:"vram_gb"`
	Status            string  `json:"status"`
	ReputationScore   float64 `json:"reputation_score"`
	PCIBusID          string  `json:"pci_bus_id"`
	Temperature       int     `json:"temperature_c,omitempty"`
	Utilization       int     `json:"utilization_pct,omitempty"`
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
	Type                  string          `json:"type"`
	JobID                 string          `json:"job_id"`
	CapabilityFingerprint string          `json:"capability_fingerprint,omitempty"`
	HashMode              int             `json:"hash_mode"`
	Target                string          `json:"target"`
	Dictionary            *DictionarySpec `json:"dictionary,omitempty"`
	TargetURL             string          `json:"target_url,omitempty"`
	TargetSHA256          string          `json:"target_sha256,omitempty"`
	TargetFilename        string          `json:"target_filename,omitempty"`
	ArtifactID            string          `json:"artifact_id,omitempty"`
	TargetCanary          string          `json:"target_canary,omitempty"`
	TaskType              string          `json:"task_type,omitempty"`
	VerificationType      string          `json:"verification_type,omitempty"`
	ChallengeID           string          `json:"challenge_id,omitempty"`
	VerificationRequired  bool            `json:"verification_required,omitempty"`
	VerificationEpochID   string          `json:"verification_epoch_id,omitempty"`
	KeyspaceContract      json.RawMessage `json:"keyspace_contract,omitempty"`
	Skip                  int64           `json:"skip"`
	Limit                 int64           `json:"limit"`
	ParentJobID           string          `json:"parent_job_id,omitempty"`
	ChunkIndex            int             `json:"chunk_index,omitempty"`
}

type DictionarySpec struct {
	DictID         string `json:"dict_id,omitempty"`
	DictURL        string `json:"dict_url,omitempty"`
	CompressFormat string `json:"compress_format,omitempty"`
	Checksum       string `json:"checksum,omitempty"`
	LineCount      int64  `json:"line_count,omitempty"`
	RuntimePath    string `json:"-"`
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
	Type                       string `json:"type"`
	MinerID                    string `json:"miner_id,omitempty"`
	Status                     string `json:"status"`
	VerificationRequired       bool   `json:"verification_required,omitempty"`
	VerificationEpochID        string `json:"verification_epoch_id,omitempty"`
	VerificationState          string `json:"verification_state,omitempty"`
	VerificationDeferredReason string `json:"verification_deferred_reason,omitempty"`
	MigrationStatus            string `json:"migration_status,omitempty"`
	StakeRecovered             bool   `json:"stake_recovered,omitempty"`
}

type OTAUpdateMessage struct {
	Type          string   `json:"type"`
	LatestVersion string   `json:"latest_version"`
	DownloadURLs  []string `json:"download_urls"`
	Checksum      string   `json:"checksum"`
}

type SendProbeMessage struct {
	Type        string `json:"type"`
	ChallengeID string `json:"challenge_id"`
	Payload     []byte `json:"payload"`
}

type HashcatCapabilityProbeMessage struct {
	Type                  string               `json:"type"`
	ProbeID               string               `json:"probe_id"`
	CapabilityFingerprint string               `json:"capability_fingerprint"`
	JobShape              HashcatProbeJobShape `json:"job_shape"`
	ProbePayload          HashcatProbePayload  `json:"probe_payload"`
	TimeoutMS             int                  `json:"timeout_ms,omitempty"`
}

type HashcatProbeJobShape struct {
	HashMode         int             `json:"hash_mode"`
	AttackMode       *int            `json:"attack_mode,omitempty"`
	Dictionary       *DictionarySpec `json:"dictionary,omitempty"`
	KeyspaceContract json.RawMessage `json:"keyspace_contract,omitempty"`
}

type HashcatProbePayload struct {
	TargetSample string   `json:"target_sample"`
	Args         []string `json:"args,omitempty"`
}

type HashcatCapabilityProbeResultMessage struct {
	Type                  string `json:"type"`
	ProbeID               string `json:"probe_id"`
	CapabilityFingerprint string `json:"capability_fingerprint"`
	Status                string `json:"status"`
	ReasonCode            string `json:"reason_code"`
	HashcatVersion        string `json:"hashcat_version,omitempty"`
	ErrorSummary          string `json:"error_summary,omitempty"`
}
