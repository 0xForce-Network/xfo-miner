# xfo-miner

`xfo-miner` is the 0xForce compute orchestration client (BYOH: Bring Your Own Hashcat/Hardware).

The client is responsible for:
- Environment self-check (GPU / Hashcat / Docker / Nvidia Container Toolkit)
- WebSocket communication with the mining pool (login, heartbeat, task messages, result submission)
- Smart scheduling state machine (`PRE_HEAT_STANDBY` / `WPA_AUDIT` / `AI_CONTAINER`)
- Managed subprocess lifecycle (start, streaming logs, SIGTERM→SIGKILL)

## System Requirements

- Go 1.22+
- Optional dependencies (by run mode):
  - `hashcat`
  - `docker`
  - `cloudflared`
  - GPU driver tools: `nvidia-smi` or `clinfo`

Missing dependencies will cause automatic degradation to `CPU_ONLY` mode without crashing.

## Quick Start

```bash
cp config.example.json config.json
go run ./cmd/xfo-miner -config ./config.json
```

## Build & Release

```bash
make build        # linux/amd64
make build-all    # linux/windows/darwin
make package      # generate .tar.gz/.zip
make checksums    # generate SHA256SUMS
make release      # clean + checksums (full release workflow)
```

Release artifacts are output to `bin/`:

- `xfo-miner-linux-amd64`
- `xfo-miner-windows-amd64.exe`
- `xfo-miner-darwin-arm64`
- `xfo-miner-linux-amd64.tar.gz`
- `xfo-miner-windows-amd64.zip`
- `xfo-miner-darwin-arm64.tar.gz`
- `SHA256SUMS`

## Config

Configuration structure follows `docs/miner/0xforce_miner_specs.md` §4.

Field descriptions:
- `node_id`: Node identifier (required)
- `worker_name`: Worker node name (required)
- `pool_url`: Pool address (required, must be `wss://`)
- `max_cpu_threads`: CPU thread limit for `xfo-miner` task execution, and also the default full-mode cap for xmrig when `cpu_mining.max_threads` is omitted (optional, defaults to `runtime.NumCPU()/2`, minimum 1)
- `cpu_mining`: XMRig CPU mining configuration
  - `enabled`: Whether to enable XMRig background mining
  - `xmrig_path`: Path to the xmrig binary
  - `stratum_url`: Stratum pool URL for xmrig
  - `max_threads`: XMRig threads in full mining mode
  - `background_threads`: XMRig threads in heartbeat/standby mode
  - `extra_args`: Optional JSON string array of additional xmrig arguments, for example `["--proxy=127.0.0.1:1080"]` for SOCKS5
    - Reserved flags managed by `xfo-miner` are rejected, including pool/auth/algo/thread/HTTP API flags such as `-o/-u/-p/-a/--threads/--http-port`
- `idle_behavior`: Idle state subprocess configuration
  - `enabled`: Whether to enable idle miner; keep this `false` unless you intentionally want a standby subprocess
  - `grace_period_sec`: Grace period in seconds before stopping idle miner
  - `command`: Idle miner executable command
  - `args`: Idle miner argument string

Windows path note:
- `config.json` is strict JSON, so Windows paths must use forward slashes or escaped backslashes.
- Valid examples:
  - `"xmrig_path": "./xmrig.exe"`
  - `"hashcat_path": "C:\\hashcat\\hashcat.exe"`

See: `docs/config_reference.md`.

## Run Modes

- `GPU_FULL`: GPU + Hashcat + Docker + NVIDIA container capabilities all available
- `GPU_HASHCAT_ONLY`: GPU + Hashcat available, but container capabilities insufficient
- `CPU_ONLY`: All other cases (graceful degradation)

## Scheduler States

- `PRE_HEAT_STANDBY`: Running idle miner, waiting for tasks
- `WPA_AUDIT`: Stops idle miner, executes Hashcat task, reports progress/results
- `AI_CONTAINER`: Stops idle miner, launches container and tunnel, reports temporary URL

State transitions:
- `STANDBY -> WPA_AUDIT -> STANDBY`
- `STANDBY -> AI_CONTAINER -> STANDBY`

## Development

```bash
make test
make vet
go test -race ./...
```
