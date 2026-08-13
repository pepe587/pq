# pipeline-queue — Design Spec

**Fecha:** 2026-08-14
**Estado:** Aprobado para planificación de implementación

## Resumen

CLI en Python que ejecuta pipelines declarativos (YAML) **uno a la vez (FIFO)**, con cada pipeline en su propia carpeta self-contained. Estado de la cola en SQLite, base de datos propia por pipeline para memoria persistente entre ejecuciones, logs por run, reintentos con backoff, cooldown entre ejecuciones del mismo pipeline, contador diario de uploads, y skip-if-exists para idempotencia.

El objetivo es automatizar la generación de contenido (videos para YouTube y similares) donde múltiples APIs/modelos locales deben ejecutarse en secuencia respetando cuotas, rate limits y límites anti-spam.

## Objetivos y no-objetivos

### Objetivos

- Ejecutar pipelines YAML declarativos de forma robusta y reproducible.
- Garantizar **1 pipeline completo a la vez** (FIFO estricto).
- Persistir estado de la cola entre reinicios.
- Ofrecer **memoria persistente por pipeline** vía SQLite propia.
- Manejar errores con reintentos y backoff sin intervención manual.
- Respetar **cooldown** entre ejecuciones del mismo pipeline.
- Respetar **cuota diaria** de uploads a YouTube.
- Reanudar runs fallados sin regenerar outputs ya producidos.
- CLI simple y operativa sin servicios adicionales (sin web UI, sin daemon externo).

### No-objetivos

- **No paraleliza entre pipelines.** FIFO estricto.
- **No paraleliza iteraciones dentro de un step.** Serie.
- **No gestiona VRAM.** Cada comando es responsable de liberar recursos al exit.
- **No mide VRAM disponible.** El usuario conoce sus modelos.
- **No provee UI web.**
- **No hace scheduling tipo cron.** El usuario encola manualmente.
- **No notifica a servicios externos** (Discord, Slack, email).
- **No decide en qué GPU corre cada cosa.** Usa `CUDA_VISIBLE_DEVICES` del entorno del worker.

## Arquitectura

### Vista general

```
┌──────────┐    encola    ┌──────────────┐
│   CLI    │ ───────────► │  SQLite cola │
│ (pq add) │              │  (~/.local)  │
└──────────┘              └──────────────┘
                                  ▲
                                  │ lee/escribe
                                  │
                          ┌───────┴────────┐
                          │    Worker      │
                          │  (pq daemon)   │
                          │                │
                          │  - FIFO        │
                          │  - reintentos  │
                          │  - cooldown    │
                          │  - skip-if-ex. │
                          └───────┬────────┘
                                  │ lanza subprocess
                                  ▼
                          ┌───────────────┐
                          │   pipeline/   │
                          │   - yaml      │
                          │   - prompts/  │
                          │   - outputs/  │
                          │   - scripts/  │
                          └───────┬───────┘
                                  │ accede vía env vars
                                  ▼
                          ┌───────────────┐
                          │  SQLite del   │
                          │   pipeline    │
                          │ (memoria)     │
                          └───────────────┘
```

### Componentes

| Componente | Responsabilidad |
|------------|-----------------|
| `pq/cli.py` | Entry point Click/Typer. Subcomandos: `add`, `list`, `logs`, `retry`, `cancel`, `daemon`. |
| `pq/config.py` | Carga `~/.config/pq/config.toml`. |
| `pq/db.py` | SQLite de la cola + migraciones de schema. |
| `pq/pipelines.py` | Parseo y validación de `pipeline.yaml`. Snapshot al `add`. |
| `pq/runner.py` | Ejecución de steps. Subprocess. Reintentos con backoff. Captura de logs. |
| `pq/scheduler.py` | Decide qué run corre ahora. Respeta cooldown y cuota. |
| `pq/cancel.py` | Manejo de SIGTERM/SIGINT y cancelaciones. |
| `pq/signals.py` | Helpers para registrar handlers de señales. |
| `tests/` | Tests unitarios con pytest. |

## Estructura en disco

### Pipelines (gestionadas por el usuario)

```
pipelines/
└── youtube-video/
    ├── pipeline.yaml
    ├── prompts/
    │   └── img_{i}.txt
    ├── outputs/
    │   ├── guion.txt
    │   ├── imagenes/
    │   │   └── img_{i}.png
    │   ├── clips/
    │   │   └── clip_{i}.mp4
    │   ├── audio.wav
    │   └── final.mp4
    └── scripts/
        └── montaje.sh
```

- **`pipeline.yaml`**: definición del pipeline (ver formato abajo).
- **`prompts/`**: inputs persistentes (los genera un step anterior o los deja el usuario).
- **`outputs/`**: outputs persistentes entre runs. La cola **no** borra nada aquí.
- **`scripts/`**: scripts auxiliares referenciados desde el YAML.

### Datos de la cola (gestionados por `pq`)

```
~/.local/share/pq/
├── pq.db                           # estado de la cola
├── db/                             # BD por pipeline
│   └── youtube-video.db
└── runs/<run_id>/
    ├── meta.json                   # inputs, timestamps, status, snapshot del YAML
    └── steps/<step_id>/<i>/
        ├── log.txt                 # stdout+stderr del subprocess
        └── exit_code               # 0 si éxito

~/.config/pq/
└── config.toml                     # configuración global
```

## Formato del pipeline

```yaml
name: youtube-video
cooldown: 4h
inputs:
  topic:
    type: string
    required: true

steps:
  - id: guion
    command: ollama
    args: ["run", "cloud", "guion sobre {topic}"]
    iterates:
      count: 6
      out_template: prompts/img_{i}.txt
    produces:
      - prompts/img_{i}.txt
      - outputs/guion.txt

  - id: imagenes
    needs: [guion]
    command: ideogram
    args: ["--prompt-file", "{prompts}"]
    iterates:
      count_from: prompts/img_*.txt
    produces:
      - outputs/imagenes/img_{i}.png

  - id: clips
    needs: [imagenes]
    command: minimax
    args:
      - "--image"
      - "{imagenes}"
      - "--prompt-file"
      - "{prompts}"
    iterates:
      count_from: outputs/imagenes/img_*.png
    produces:
      - outputs/clips/clip_{i}.mp4

  - id: audio
    needs: [guion]
    command: fish
    args: ["--text-file", "outputs/guion.txt"]
    produces:
      - outputs/audio.wav

  - id: montaje
    needs: [clips, audio]
    command: bash
    args: ["scripts/montaje.sh"]
    produces:
      - outputs/final.mp4

  - id: upload
    type: upload
    needs: [montaje]
    command: yt-upload
    args: ["--video", "outputs/final.mp4"]
```

### Campos del YAML

| Campo | Tipo | Descripción |
|-------|------|-------------|
| `name` | string | Identificador único del pipeline. Se usa como nombre de la BD. |
| `cooldown` | duration | Tiempo mínimo entre ejecuciones exitosas. Default: `0s`. |
| `inputs` | map | Inputs del run. Cada uno tiene `type` y `required`. |
| `steps[].id` | string | Identificador único dentro del pipeline. |
| `steps[].command` | string | Ejecutable a invocar. Se busca en `PATH`. |
| `steps[].args` | list[string] | Argumentos. Soporta placeholders `{i}`, `{topic}`, `{imagenes}`, etc. |
| `steps[].needs` | list[string] | IDs de steps que deben completarse antes. Default: `[]`. |
| `steps[].iterates` | object | Configuración de iteración (ver abajo). Si no está, el step corre 1 vez. |
| `steps[].produces` | list[string] | Paths que el step produce. Soporta `{i}`. Usado para skip-if-exists. |
| `steps[].type` | string | Marcador semántico. Valores: `upload` (cuenta hacia cuota diaria). |

### Iteración

`iterates` puede tener:

- **`count: N`** — el step corre exactamente N veces. Se usa con `out_template` para localizar el output de cada iteración.
- **`count_from: <glob>`** — el step corre una vez por archivo que matchea el glob. El path resuelto se inyecta como `{prompts}`, `{imagenes}`, etc.

Cuando un step con `count: N` declara `produces: outputs/imagenes/img_{i}.png`, `pq` comprueba para cada `i ∈ [1..N]` si el archivo existe. Si existe, se salta la iteración.

### Resolución de placeholders en `args`

- `{i}` — índice de la iteración (1..N).
- `{topic}` — input declarado (string).
- `{imagenes}`, `{prompts}`, etc. — referencia a los outputs del step con ese `id`. Se resuelve como **lista de paths**. Si el comando acepta un solo argumento por archivo, `pq` itera y lanza el comando N veces (una por archivo). Si acepta una lista, se pasa como múltiples args.

### Variables de entorno inyectadas a cada step

- `PQ_DB_PATH` — ruta absoluta a la BD del pipeline (`~/.local/share/pq/db/<name>.db`). El archivo es creado por `pq` si no existe.
- `PQ_PIPELINE_DIR` — ruta absoluta a la carpeta del pipeline. Es el `cwd` del subprocess.
- `PQ_RUN_ID` — id del run actual.
- `PQ_INPUT_<KEY>` — un env var por cada input declarado en el YAML, con la clave en mayúsculas.

## Flujo de un run

### 1. Adición (`pq add`)

1. `pq` lee y valida el YAML **fail fast**:
   - `name` único.
   - Todos los `needs` referencian steps existentes.
   - No hay ciclos en el grafo de dependencias.
   - Todos los inputs requeridos se pasan por CLI.
2. Crea un **snapshot** del YAML en `runs/<run_id>/meta.json`.
3. Inserta un run en SQLite con status `queued`, `cooldown_until` calculado desde runs anteriores del mismo pipeline.

### 2. Selección (`pq daemon`)

El worker, en bucle:

1. Lee el siguiente run con status `queued` (o `waiting`) y `cooldown_until <= now` (ordenado por `created_at` FIFO).
2. Si hay un step `type: upload` y `uploads_today >= max_uploads_per_day`, marca el run como `waiting` y salta.
3. Si no hay run elegible, espera (polling cada 30s por defecto, configurable).

### 3. Ejecución

Para cada step del run, en orden topológico (respeta `needs`):

1. Resuelve `iterates.count` o `iterates.count_from` (glob contra `PQ_PIPELINE_DIR`).
2. Para cada iteración `i`:
   - Comprueba `produces` para esa iteración. Si todos los outputs existen en disco, marca la iteración como `skipped`.
   - Si falta algún output, lanza el `command` como subprocess con:
     - `cwd = PQ_PIPELINE_DIR`
     - `env = os.environ + vars inyectadas por pq`
     - `args` con placeholders resueltos.
   - Captura `stdout+stderr` a `runs/<run_id>/steps/<step_id>/<i>/log.txt`.
   - `exit_code` se guarda en `runs/<run_id>/steps/<step_id>/<i>/exit_code`.
3. Si todas las iteraciones son `skipped` o `done`, marca el step como `done`.
4. Si alguna iteración falla:
   - `attempts++` (por step, no por iteración).
   - Backoff: 30s, 2m, 10m.
   - Hasta `max_attempts: 3` (configurable en `config.toml`).
   - Si se agotan, el run queda `failed`. Outputs parciales **se conservan**.

### 4. Finalización

- **Éxito**: run `done`, `cooldown_until = now + pipeline.cooldown`.
- **Fallo**: run `failed`, `cooldown_until` no se actualiza (puedes reintentar inmediatamente).
- **Cancelación**: run `cancelled`, outputs parciales se conservan.

## Manejo de errores y reintentos

### Reintentos automáticos

Configurables en `~/.config/pq/config.toml`:

```toml
[retry]
max_attempts = 3
backoff = [30, 120, 600]  # segundos entre intentos
```

### Cancelación

- `pq cancel <run_id>` envía SIGKILL al subprocess activo del run.
- Outputs parciales (los que ya estaban en `outputs/` del pipeline) se quedan.
- El run queda en estado `cancelled`.
- Si se cancela un run `waiting` o `queued`, se marca `cancelled` directamente sin lanzar nada.

### Señales al worker

- `Ctrl+C` en `pq daemon` (SIGINT): el worker deja de aceptar nuevos runs y **termina el step actual de forma ordenada** (espera a que el subprocess termine naturalmente). Luego sale.
- `SIGTERM`: mismo comportamiento que SIGINT.

## Cuota diaria y cooldown

### Cooldown por pipeline

- Declarado en `pipeline.yaml` como `cooldown: 4h`.
- Se aplica entre **finalización exitosa** de un run y el inicio del siguiente.
- Runs fallidos no activan cooldown (puedes reintentar de inmediato).

### Cuota YouTube

Config global:

```toml
[quota]
max_uploads_per_day = 3
```

- Solo cuentan los steps con `type: upload`.
- Contador en SQLite: tabla `counters` con `(day, uploads_count)`.
- Si al ejecutar un run el step `type: upload` excedería la cuota, el run queda `waiting` hasta medianoche local.
- A medianoche, el contador se resetea automáticamente.

## Persistencia

### SQLite de la cola (`~/.local/share/pq/pq.db`)

Tablas:

```sql
CREATE TABLE runs (
    id INTEGER PRIMARY KEY,
    pipeline_name TEXT NOT NULL,
    pipeline_dir TEXT NOT NULL,    -- ruta absoluta, snapshot
    inputs_json TEXT NOT NULL,
    status TEXT NOT NULL,          -- queued, waiting, running, done, failed, cancelled
    created_at TEXT NOT NULL,
    started_at TEXT,
    finished_at TEXT,
    cooldown_until TEXT,
    error TEXT
);

CREATE TABLE steps (
    id INTEGER PRIMARY KEY,
    run_id INTEGER NOT NULL REFERENCES runs(id),
    step_id TEXT NOT NULL,         -- id dentro del pipeline
    needs_json TEXT,
    iterates_json TEXT,
    produces_json TEXT,
    type TEXT,                     -- 'upload' o NULL
    status TEXT NOT NULL,          -- pending, running, done, failed
    attempts INTEGER DEFAULT 0,
    UNIQUE(run_id, step_id)
);

CREATE TABLE step_iterations (
    id INTEGER PRIMARY KEY,
    step_run_id INTEGER NOT NULL REFERENCES steps(id),
    iteration INTEGER NOT NULL,    -- 1..N
    status TEXT NOT NULL,          -- pending, running, done, skipped, failed
    log_path TEXT,
    exit_code INTEGER,
    UNIQUE(step_run_id, iteration)
);

CREATE TABLE counters (
    day TEXT PRIMARY KEY,          -- YYYY-MM-DD local
    uploads_count INTEGER DEFAULT 0
);
```

### SQLite por pipeline (`~/.local/share/pq/db/<name>.db`)

- Creada por `pq` al primer run de ese pipeline (archivo vacío).
- Schema gestionado por el pipeline (el primer step puede hacer `CREATE TABLE IF NOT EXISTS`).
- `pq` no toca esta DB salvo para asegurar que existe.
- Sirve como **memoria persistente entre ejecuciones**: el pipeline puede guardar temas usados, resultados, estadísticas, etc.

## CLI

### Subcomandos

```
pq add <pipeline-dir> [--input key=value ...]
    Encola un nuevo run. Valida el YAML fail fast. Crea snapshot.

pq list [--status queued|waiting|running|done|failed|cancelled]
    Lista runs. Default: últimos 20.

pq logs <run_id> [step_id]
    Muestra logs de un run o step. Si step_id se omite, muestra todos.

pq retry <run_id>
    Reintenta un run failed. Lo pone en queued. Reanuda desde el último step no done.

pq cancel <run_id>
    Cancela un run activo. SIGKILL al subprocess. Conserva outputs.

pq daemon
    Arranca el worker en foreground. Ctrl+C para parar ordenadamente.
```

### Flags globales

- `--config <path>` — ruta alternativa a `config.toml`.
- `--data-dir <path>` — ruta alternativa a `~/.local/share/pq/`.

## Configuración global (`~/.config/pq/config.toml`)

```toml
[general]
data_dir = "~/.local/share/pq"
log_level = "info"

[worker]
poll_interval_seconds = 30

[retry]
max_attempts = 3
backoff = [30, 120, 600]  # segundos

[quota]
max_uploads_per_day = 3
timezone = "Europe/Madrid"  # para reset diario
```

## Validación del YAML (fail fast en `add`)

Errores que rechazan el `add`:

- `name` duplicado (ya existe una `runs/<id>/meta.json` con ese nombre activo) — *no, esto se permite, ver "lock del pipeline"*.
- `name` ausente o vacío.
- Step sin `id` o con `id` duplicado.
- Step con `needs` que referencia un id inexistente.
- Ciclo en el grafo de `needs` (detectado por DFS).
- Input requerido ausente en CLI.
- Comando vacío o no resoluble (no se chequea PATH hasta ejecutar).
- `count` y `count_from` ambos presentes o ambos ausentes en `iterates`.
- Glob inválido en `count_from`.

## Comportamientos edge case

### Mismo pipeline encolado múltiples veces

- Se permiten múltiples runs simultáneos en cola. La cola los procesa FIFO.
- No hay "lock" del pipeline: si encolas el mismo pipeline dos veces, corren en serie (uno tras otro).

### Cambio del `pipeline.yaml` durante un run en curso

- El run usa el **snapshot** tomado al `add`, guardado en `runs/<run_id>/meta.json`.
- Cambios posteriores al YAML no afectan al run en curso.

### Step que no produce los archivos esperados

- Si tras `done` los archivos en `produces` no existen, no es error de `pq`. Se asume éxito y se continúa.
- El usuario debe ser responsable de que el comando produzca lo que declara.

### Outputs preexistentes al primer run

- Si `outputs/imagenes/img_1.png` ya existe antes del primer run, el step `imagenes` lo detecta y lo salta. Esto permite **empezar un run a mitad** si ya tienes outputs válidos.

### Cancelación durante backoff

- Si cancelas un run que está en espera de backoff, se cancela inmediatamente. No espera al reintento.

### Worker sin runs

- Hace polling cada `poll_interval_seconds`. No consume recursos significativos.

## Testing

### Tests unitarios (`tests/`)

- `test_pipelines.py` — parseo y validación de YAML, detección de ciclos.
- `test_runner.py` — ejecución de steps, skip-if-exists, reintentos.
- `test_scheduler.py` — selección FIFO, respeto de cooldown y cuota.
- `test_db.py` — migraciones, contadores.
- `test_cancel.py` — señales, cancelaciones.

### Tests de integración

- Pipeline de ejemplo con 2-3 steps simples (echo, cp, sleep) para verificar flujo end-to-end.
- Mock de comandos para tests de fallos y reintentos.

## YAGNI explícito

Estas features se mencionan para descartarlas formalmente:

- ~~Paralelismo entre pipelines.~~
- ~~Paralelismo dentro de un step.~~
- ~~Medición de VRAM.~~
- ~~Web UI / dashboard.~~
- ~~Scheduler tipo cron.~~
- ~~Notificaciones externas.~~
- ~~Selección de GPU.~~
- ~~Auto-descubrimiento de pipelines.~~ (siempre se encolan por path explícito).
- ~~Versionado de pipelines (git integration).~~ (queda como YAGNI; el snapshot basta).

## Riesgos y mitigaciones

| Riesgo | Mitigación |
|--------|-----------|
| Subprocess que no respeta SIGTERM | `pq cancel` usa SIGKILL directo, sin esperar. |
| Disco lleno | `pq` no comprueba espacio. El usuario debe monitorizar. Se documenta en README. |
| Pipeline malicioso que borra `~/.local/share/pq/` | Documentado como caveat. La cola confía en el usuario (es un tool local). |
| Horario de reset de cuota incorrecto | Configurable vía `timezone` en `config.toml`. |
| Outputs en `produces` que el comando no crea | No se valida post-ejecución. El usuario declara responsablemente. |
| Glob en `count_from` con muchos archivos | Se itera sin límite. El usuario debe ser sensato. Si quiere, puede añadir un `max_iterations` en el futuro. |

## Open questions

Ninguna. Todas las decisiones de diseño están tomadas.
