# xfo-miner

`xfo-miner` 是 0xForce 的算力編排客戶端（BYOH：Bring Your Own Hashcat/Hardware）。

客戶端負責：
- 環境自檢（GPU / Hashcat / Docker / Nvidia Container Toolkit）
- 與礦池 WebSocket 通訊（登入、心跳、任務訊息、結果回傳）
- 智慧排程狀態機（`PRE_HEAT_STANDBY` / `WPA_AUDIT` / `AI_CONTAINER`）
- 受控子程序生命週期管理（啟動、串流日誌、SIGTERM→SIGKILL）

## 系統需求

- Go 1.22+
- 選用依賴（依執行模式）：
  - `hashcat`
  - `docker`
  - `cloudflared`
  - GPU 驅動工具：`nvidia-smi` 或 `clinfo`

依賴缺失時會自動降級至 `CPU_ONLY`，不會崩潰退出。

## 快速開始

```bash
cp config.example.json config.json
go run ./cmd/xfo-miner -config ./config.json
```

## 建置與發佈

```bash
make build        # linux/amd64
make build-all    # linux/windows/darwin
make package      # 產生 .tar.gz/.zip
make checksums    # 產生 SHA256SUMS
make release      # clean + checksums（完整發佈流程）
```

發佈產物輸出至 `bin/`：

- `xfo-miner-linux-amd64`
- `xfo-miner-windows-amd64.exe`
- `xfo-miner-darwin-arm64`
- `xfo-miner-linux-amd64.tar.gz`
- `xfo-miner-windows-amd64.zip`
- `xfo-miner-darwin-arm64.tar.gz`
- `SHA256SUMS`

## 設定

設定結構遵循 `docs/miner/0xforce_miner_specs.md` §4。

欄位說明：
- `node_id`：節點識別碼（必填）
- `worker_name`：工作節點名稱（必填）
- `pool_url`：礦池位址（必填，必須為 `wss://`）
- `max_cpu_threads`：CPU 執行緒上限（選填，預設 `runtime.NumCPU()/2`，最小 1）
- `idle_behavior`：閒置狀態子程序設定
  - `enabled`：是否啟用閒置礦工
  - `grace_period_sec`：停止閒置礦工的寬限秒數
  - `command`：閒置礦工可執行命令
  - `args`：閒置礦工參數字串

詳見：`docs/config_reference.md`。

## 執行模式

- `GPU_FULL`：GPU + Hashcat + Docker + NVIDIA 容器能力齊備
- `GPU_HASHCAT_ONLY`：GPU + Hashcat 可用，但容器能力不足
- `CPU_ONLY`：其餘情況（優雅降級）

## 排程器狀態

- `PRE_HEAT_STANDBY`：執行閒置礦工，等待任務
- `WPA_AUDIT`：停止閒置礦工，執行 Hashcat 任務，回報進度/結果
- `AI_CONTAINER`：停止閒置礦工，啟動容器與隧道，回報臨時 URL

狀態遷移：
- `STANDBY -> WPA_AUDIT -> STANDBY`
- `STANDBY -> AI_CONTAINER -> STANDBY`

## 開發

```bash
make test
make vet
go test -race ./...
```
