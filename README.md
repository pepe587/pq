# pq — pipeline-queue

Un ejecutor FIFO en cola para pipelines de IA declarativos. Diseñado para correr
en una sola GPU (Tesla V100 32GB en el caso del autor) ejecutando pipelines que
consumen mucha VRAM de forma secuencial, sin offload entre ellos.

Cada pipeline se declara en un `pipeline.yaml`, se encola con `pq add`, y un
proceso `pq daemon` los va cogiendo en orden FIFO, ejecutando sus steps, y
aplicando cooldown por pipeline más una quota diaria de uploads a YouTube.

## Instalación

```bash
cd /home/pepe/Desktop/pipeline-queue
python -m venv .venv
.venv/bin/pip install -e .
```

Entry point: `pq` (módulo `pq.cli:main`).

## Quick start

```bash
# 1. Encolar un pipeline con un input
.venv/bin/pq --data-dir ~/.local/share/pq add examples/youtube-video \
    --input topic="IA en 2026"

# 2. Arrancar el worker en foreground (Ctrl+C para parar limpio)
.venv/bin/pq --data-dir ~/.local/share/pq daemon
```

## Estructura de directorios

Cada pipeline vive en su propio directorio, self-contained:

```
mi-pipeline/
├── pipeline.yaml         # declaración del pipeline (obligatorio)
├── prompts/              # inputs generados por el step `guion`
├── outputs/              # outputs por step (creados al vuelo)
└── ...                   # lo que necesite el pipeline
```

El runner ejecuta cada step con `cwd=<pipeline_dir>`, así que los paths en
`args` y `produces` son relativos al directorio del pipeline.

## Esquema de `pipeline.yaml`

```yaml
name: mi-pipeline                  # nombre único
cooldown: 4h                       # espera mínima entre runs exitosos (duración)
inputs:                            # parámetros que se pasan al añadir
  topic:
    type: string
    required: true
  num_images:
    type: int
    required: false                # default: no必填

steps:
  - id: guion                      # identificador único dentro del pipeline
    command: sh                    # comando a ejecutar (argv[0])
    args: ["-c", "echo {topic} > outputs/guion.txt"]
    produces: ["outputs/guion.txt"]
    iterates:                      # opcional: ejecutar el step N veces
      count: 6                     # o count_from: <glob>
      out_template: outputs/img_{i}.txt   # cómo nombrar cada output

  - id: imagenes
    needs: [guion]                 # dependencias (orden topológico)
    command: ideogram
    args: ["--prompt", "{i}", "--out", "outputs/imgs/img_{i}.png"]
    produces: ["outputs/imgs/img_{i}.png"]
    iterates:
      count: 6

  - id: clips
    needs: [imagenes]
    command: minimax
    args: ["--image", "outputs/imgs/img_{i}.png", "--out", "outputs/clips/clip_{i}.mp4"]
    produces: ["outputs/clips/clip_{i}.mp4"]
    iterates:
      count: 6

  - id: audio
    needs: [guion]
    command: fish-speech
    args: ["--text", "outputs/guion.txt", "--out", "outputs/audio.wav"]
    produces: ["outputs/audio.wav"]

  - id: montaje
    needs: [clips, audio]
    command: ffmpeg
    args: ["-y", "-i", "outputs/clips/clip_%d.mp4", "-i", "outputs/audio.wav", "outputs/final.mp4"]
    produces: ["outputs/final.mp4"]

  - id: upload                     # type=upload cuenta contra la quota
    type: upload
    needs: [montaje]
    command: ./scripts/upload.sh
    args: ["outputs/final.mp4"]
    produces: ["outputs/uploaded.txt"]
```

### Campos de un step

| Campo      | Tipo                | Obligatorio | Notas                                           |
|------------|---------------------|-------------|-------------------------------------------------|
| `id`       | str                 | sí          | único dentro del pipeline                       |
| `command`  | str                 | sí          | argv[0]; usar `sh -c "..."` para composición    |
| `args`     | list[str]           | sí          | cada arg se pasa literal al execvp              |
| `needs`    | list[str]           | no          | ids de steps que deben completarse antes        |
| `produces` | list[str]           | no          | paths relativos a `pipeline.dir`; si existen, skip |
| `iterates` | {count, count_from} | no          | ver "Iteración" abajo                           |
| `type`     | "regular"\|"upload" | no          | default "regular"; "upload" consume quota       |

### Iteración: `count` vs `count_from`

- `count: N` — el step se ejecuta N veces, `{i}` se sustituye por 1..N.
- `count_from: "prompts/*.txt"` — el step se ejecuta una vez por archivo que
  matchee el glob, en orden lexicográfico. Útil cuando el número de iteraciones
  lo decide el output de un step anterior.

En ambos casos el runner ejecuta **todos los archivos** (o todos los N); un
fallo en cualquier iteración aborta y se considera fallo del step entero, que se
reintenta desde cero (no por iteración).

### Placeholders

- `{i}` — índice de iteración (1..N o derivado del stem del archivo en `count_from`).
- `{topic}`, `{num_images}`, etc. — valores de `inputs` del pipeline.

**No se sustituyen** otros placeholders como `{prompts}` o `{imagenes}` (el
runner no resuelve referencias entre steps; el step siguiente lee los outputs
del anterior directamente desde disco).

### Duración del `cooldown`

Acepta `Ns`, `Nm`, `Nh` (segundos, minutos, horas). Ejemplos: `"30s"`, `"5m"`,
`"4h"`. `0s` desactiva el cooldown.

## Variables de entorno que recibe cada step

El runner exporta estas vars antes de cada `subprocess.run`:

- `PQ_RUN_ID` — id numérico del run en la cola
- `PQ_DB_PATH` — ruta al SQLite del pipeline (`<data_dir>/db/<name>.db`)
- `PQ_PIPELINE_DIR` — `cwd` del step (= directorio del pipeline)
- `PQ_INPUT_<KEY>` — para cada input, en mayúsculas (`PQ_INPUT_TOPIC`, etc.)

## CLI

Todos los subcomandos aceptan `--config <path>` y `--data-dir <path>` (sobreescriben
el `~/.config/pq/config.toml`).

```bash
# Añadir un pipeline a la cola
pq add <pipeline_dir> [--input key=value]...

# Listar runs (más recientes primero; --status filtra; --limit N)
pq list [--status queued] [--limit 20]

# Mostrar logs de un run o step concreto
pq logs <run_id> [<step_id>]

# Reintentar un run fallido
pq retry <run_id>

# Cancelar un run activo (SIGKILL al subprocess, marca cancelled, conserva outputs)
pq cancel <run_id>

# Worker en foreground (Ctrl+C = stop limpio entre runs)
pq daemon
```

## Configuración (`~/.config/pq/config.toml`)

```toml
data_dir = "~/.local/share/pq"        # obligatorio
poll_interval_seconds = 5              # pausa cuando no hay runs
max_uploads_per_day = 3                # quota de YouTube
max_attempts = 3                       # reintentos por step
backoff = [30, 120, 600]               # segundos entre reintentos
timezone = "Europe/Madrid"             # para la quota diaria

[scheduler]
# Cola cíclica: el daemon recorre estos pipelines en orden y vuelve al
# principio cuando termina. Vacío = comportamiento "solo runs manuales".
cycle_pipelines = ["youtube-video", "podcast"]
```

### Scheduler cíclico

Si configuras `[scheduler] cycle_pipelines = [...]`, el daemon no solo
ejecuta lo que metas con `pq add`, sino que además rota esa lista en
bucle infinito:

1. Recorre la cola en orden.
2. Para cada pipeline, mira si su último run `done` ya pasó el cooldown
   y (si tiene steps `type: upload`) si queda quota diaria.
3. Si toca, encola un run nuevo automáticamente y lo ejecuta.
4. Cuando termina la vuelta, vuelve al primero.
5. `failed`/`cancelled` cuentan como "pasada" — el siguiente intento del
   pipeline ocurre en la siguiente vuelta (no se reintenta en la misma).

Los runs manuales (`pq add`) se intercalan en FIFO absoluto: si encolas
uno a mano mientras la cola cíclica tiene algo pendiente, gana el más
antiguo por `id`. La distinción "auto vs manual" no existe en la cola.

El puntero de rotación vive en memoria del daemon (se resetea al
rearrancar); los estados de cooldown y quota viven en la DB, así que
sobreviven a reinicios. Cada pipeline usa el `cooldown` declarado en su
propio `pipeline.yaml`.

Para registrar un pipeline en la cola cíclica, primero necesitas haberlo
ejecutado al menos una vez con `pq add` para que el daemon sepa en qué
directorio está el `pipeline.yaml`.

## Comportamiento

### FIFO estricto

Un run a la vez. Si el run A está ejecutando, el run B espera aunque su
pipeline sea distinto y no compita por VRAM.

### Cooldown

Tras un run exitoso, el siguiente run del mismo pipeline no se coge hasta que
pase el `cooldown`. Runs fallidos NO actualizan el cooldown (puedes reintentar
inmediatamente). Runs de pipelines distintos NO se ven afectados.

### Quota diaria

Steps con `type: upload` consumen una unidad de quota por día. Si la quota está
llena, el run queda en estado `waiting` y se reintenta en el siguiente poll.
El contador se reinicia cada día (medianoche en el `timezone` configurado).

### Skip-if-exists (idempotencia)

Si los archivos en `produces` ya existen en disco, el step se marca como
`skipped` y no se ejecuta. Esto permite re-ejecutar un pipeline sin regenerar
outputs que ya tienes.

### Cancelación

`pq cancel <run_id>` envía SIGKILL al subprocess activo y marca el run como
`cancelled`. Los outputs generados hasta ese momento se conservan en disco.
Si el run está esperando un backoff entre reintentos, sale del sleep
inmediatamente.

### Persistencia por pipeline

Cada pipeline tiene su propio SQLite en `<data_dir>/db/<name>.db`, accesible
desde los steps vía la env var `PQ_DB_PATH`. Útil para memoria persistente
entre runs (por ejemplo, contar cuántas veces se ha generado cada imagen).

## Desarrollo

```bash
.venv/bin/pytest -v                    # todos los tests (55 tests)
.venv/bin/pq --data-dir /tmp/pq-test add examples/youtube-video --input topic="test"
.venv/bin/pq --data-dir /tmp/pq-test daemon
```

## Layout del proyecto

```
pq/                     # paquete principal
├── cli.py              # Click CLI (add/list/logs/retry/cancel/daemon)
├── config.py           # carga de config.toml
├── db.py               # schema SQLite de la cola
├── pipeline_db.py      # helper para DB per-pipeline
├── pipelines.py        # parser + validación de pipeline.yaml
├── iterations.py       # expansión de count / count_from
├── runner.py           # subprocess execution + retries + cancel tracking
├── counter.py          # contador diario de uploads
├── scheduler.py        # FIFO pick con cooldown + quota
├── cancel.py           # cancel_run (SIGKILL + mark cancelled)
├── signals.py          # SIGINT/SIGTERM handlers
└── worker.py           # main loop del daemon

tests/                  # pytest (un archivo por módulo, mismo nombre)
examples/youtube-video/ # pipeline de ejemplo end-to-end
docs/superpowers/       # spec + plan originales
.superpowers/sdd/       # ledger de la ejecución SDD
```