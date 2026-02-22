# xfo-miner

`xfo-miner` 是 0xForce 的算力编排客户端（BYOH: Bring Your Own Hashcat/Hardware）。

客户端负责：
- 环境自检（GPU / Hashcat / Docker / Nvidia Container Toolkit）
- 与矿池 WebSocket 通信（登录、心跳、任务消息、结果回传）
- 智能调度状态机（`PRE_HEAT_STANDBY` / `WPA_AUDIT` / `AI_CONTAINER`）
- 受控子进程生命周期管理（启动、流式日志、SIGTERM→SIGKILL）

## System Requirements

- Go 1.22+
- 可选依赖（按运行模式）
  - `hashcat`
  - `docker`
  - `cloudflared`
  - GPU 驱动工具：`nvidia-smi` 或 `clinfo`

依赖缺失时会自动降级至 `CPU_ONLY`，不会崩溃退出。

## Quick Start

```bash
cp config.example.json config.json
go run ./cmd/xfo-miner -config ./config.json
```

## Build & Release

```bash
make build        # linux/amd64
make build-all    # linux/windows/darwin
make package      # 生成 .tar.gz/.zip
make checksums    # 生成 SHA256SUMS
make release      # clean + checksums（完整发布流程）
```

发布产物输出到 `bin/`：

- `xfo-miner-linux-amd64`
- `xfo-miner-windows-amd64.exe`
- `xfo-miner-darwin-arm64`
- `xfo-miner-linux-amd64.tar.gz`
- `xfo-miner-windows-amd64.zip`
- `xfo-miner-darwin-arm64.tar.gz`
- `SHA256SUMS`

## Config

配置结构遵循 `docs/miner/0xforce_miner_specs.md` §4。

字段说明：
- `node_id`：节点标识（必填）
- `worker_name`：工作节点名称（必填）
- `pool_url`：矿池地址（必填，必须 `wss://`）
- `max_cpu_threads`：CPU 线程上限（可选，默认 `runtime.NumCPU()/2`，最小 1）
- `idle_behavior`：空闲状态子进程配置
  - `enabled`：是否启用 idle miner
  - `grace_period_sec`：停止 idle miner 的宽限秒数
  - `command`：idle miner 可执行命令
  - `args`：idle miner 参数字符串

详见：`docs/config_reference.md`。

## Run Modes

- `GPU_FULL`：GPU + Hashcat + Docker + NVIDIA 容器能力齐备
- `GPU_HASHCAT_ONLY`：GPU + Hashcat 可用，但容器能力不足
- `CPU_ONLY`：其余情况（优雅降级）

## Scheduler States

- `PRE_HEAT_STANDBY`：运行 idle miner，等待任务
- `WPA_AUDIT`：停止 idle miner，执行 Hashcat 任务，上报进度/结果
- `AI_CONTAINER`：停止 idle miner，拉起容器与隧道，上报临时 URL

状态迁移：
- `STANDBY -> WPA_AUDIT -> STANDBY`
- `STANDBY -> AI_CONTAINER -> STANDBY`

## Development

```bash
make test
make vet
go test -race ./...
```
