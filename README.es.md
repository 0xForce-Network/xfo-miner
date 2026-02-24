# xfo-miner

`xfo-miner` es el cliente de orquestación de cómputo de 0xForce (BYOH: Bring Your Own Hashcat/Hardware).

El cliente se encarga de:
- Autodiagnóstico del entorno (GPU / Hashcat / Docker / Nvidia Container Toolkit)
- Comunicación WebSocket con el pool de minería (login, heartbeat, mensajes de tareas, envío de resultados)
- Máquina de estados de programación inteligente (`PRE_HEAT_STANDBY` / `WPA_AUDIT` / `AI_CONTAINER`)
- Gestión del ciclo de vida de subprocesos (inicio, logs en streaming, SIGTERM→SIGKILL)

## Requisitos del Sistema

- Go 1.22+
- Dependencias opcionales (según modo de ejecución):
  - `hashcat`
  - `docker`
  - `cloudflared`
  - Herramientas de driver GPU: `nvidia-smi` o `clinfo`

Si faltan dependencias, el sistema degrada automáticamente a modo `CPU_ONLY` sin fallar.

## Inicio Rápido

```bash
cp config.example.json config.json
go run ./cmd/xfo-miner -config ./config.json
```

## Compilación y Publicación

```bash
make build        # linux/amd64
make build-all    # linux/windows/darwin
make package      # generar .tar.gz/.zip
make checksums    # generar SHA256SUMS
make release      # clean + checksums (flujo completo de publicación)
```

Los artefactos se generan en `bin/`:

- `xfo-miner-linux-amd64`
- `xfo-miner-windows-amd64.exe`
- `xfo-miner-darwin-arm64`
- `xfo-miner-linux-amd64.tar.gz`
- `xfo-miner-windows-amd64.zip`
- `xfo-miner-darwin-arm64.tar.gz`
- `SHA256SUMS`

## Configuración

La estructura de configuración sigue `docs/miner/0xforce_miner_specs.md` §4.

Descripción de campos:
- `node_id`: Identificador del nodo (obligatorio)
- `worker_name`: Nombre del nodo trabajador (obligatorio)
- `pool_url`: Dirección del pool (obligatorio, debe ser `wss://`)
- `max_cpu_threads`: Límite de hilos CPU (opcional, por defecto `runtime.NumCPU()/2`, mínimo 1)
- `idle_behavior`: Configuración del subproceso en estado inactivo
  - `enabled`: Habilitar minero inactivo
  - `grace_period_sec`: Segundos de gracia antes de detener el minero inactivo
  - `command`: Comando ejecutable del minero inactivo
  - `args`: Cadena de argumentos del minero inactivo

Ver: `docs/config_reference.md`.

## Modos de Ejecución

- `GPU_FULL`: GPU + Hashcat + Docker + capacidades de contenedor NVIDIA disponibles
- `GPU_HASHCAT_ONLY`: GPU + Hashcat disponibles, pero sin capacidades de contenedor
- `CPU_ONLY`: Todos los demás casos (degradación elegante)

## Estados del Programador

- `PRE_HEAT_STANDBY`: Ejecutando minero inactivo, esperando tareas
- `WPA_AUDIT`: Detiene el minero inactivo, ejecuta tarea Hashcat, reporta progreso/resultados
- `AI_CONTAINER`: Detiene el minero inactivo, lanza contenedor y túnel, reporta URL temporal

Transiciones de estado:
- `STANDBY -> WPA_AUDIT -> STANDBY`
- `STANDBY -> AI_CONTAINER -> STANDBY`

## Desarrollo

```bash
make test
make vet
go test -race ./...
```
