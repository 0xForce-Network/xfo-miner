# xfo-miner 配置参考

文件名：`config.json`

示例：`../config.example.json`

## 顶层字段

### `node_id` (string, required)
- 含义：矿工节点唯一标识
- 约束：不能为空

### `worker_name` (string, required)
- 含义：工作节点名称（用于矿池展示与区分）
- 约束：不能为空

### `pool_url` (string, required)
- 含义：矿池 WebSocket 地址
- 约束：必须是合法 `wss://` URL

### `max_cpu_threads` (int, optional)
- 含义：允许调度器占用的 CPU 线程上限
- 默认值：`runtime.NumCPU()/2`（最少 1）
- 约束：`>=1` 且 `<= runtime.NumCPU()`

### `idle_behavior` (object, required)
- 含义：空闲状态下后台进程运行策略

## `idle_behavior` 子字段

### `enabled` (bool)
- 含义：是否在 STANDBY 状态启动 idle miner

### `grace_period_sec` (int)
- 含义：停止 idle miner 时发送 `SIGTERM` 后等待秒数
- 约束：不能为负数

### `command` (string)
- 含义：idle miner 执行命令
- 约束：当 `enabled=true` 时必须非空

### `args` (string)
- 含义：idle miner 命令参数（字符串形式）

## 验证行为

`internal/config.LoadConfig()` 会执行：
- JSON 解码并拒绝未知字段（`DisallowUnknownFields`）
- 默认值填充（`max_cpu_threads`）
- 必填字段与格式校验
- 线程数边界校验
- `idle_behavior` 合法性校验
