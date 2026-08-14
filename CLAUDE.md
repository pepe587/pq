# CLAUDE.md — Notas del proyecto pq (pipeline-queue)

## Qué es

CLI queue runner FIFO para pipelines de IA declarativos. Pensado para correr
pipelines pesados en VRAM de uno en uno en una sola GPU. Los pipelines se
declaran en YAML, se encolan con `pq add`, y un proceso `pq daemon` los
ejecuta respetando cooldown por pipeline y quota diaria de uploads.

## Stack

- Python 3.11+, stdlib-only (tomllib, sqlite3, subprocess, signal, zoneinfo).
- Click para CLI.
- PyYAML para parsing.
- pytest para tests.
- Sin dependencias runtime más allá de esas tres.

## Estructura de módulos

Cada módulo tiene una sola responsabilidad. Si necesitas tocar dos
responsabilities distintas, toca dos módulos.

| Módulo             | Responsabilidad                                         |
|--------------------|---------------------------------------------------------|
| `pq/cli.py`        | Click group con subcomandos; wiring de config y ctx.obj |
| `pq/config.py`     | Carga `config.toml` → dataclass `Config`                |
| `pq/db.py`         | Schema SQLite de la cola + helpers `init_db`/`get_conn` |
| `pq/pipeline_db.py`| Helper para DB per-pipeline (idempotente)               |
| `pq/pipelines.py`  | Parser + validación de `pipeline.yaml` (3-color DFS)    |
| `pq/iterations.py` | Expansión de `count` / `count_from` a `list[Iteration]` |
| `pq/runner.py`     | Ejecución subprocess con env vars, retries, PID tracking |
| `pq/counter.py`    | Contador diario de uploads (atomic upsert)              |
| `pq/scheduler.py`  | `pick_next_run` con cooldown + quota, `mark_upload_done` |
| `pq/cancel.py`     | `cancel_run`: SIGKILL subprocess + mark cancelled       |
| `pq/signals.py`    | `WorkerStop` flag + `install_handlers` para SIGINT/TERM |
| `pq/worker.py`     | Main loop del daemon (pick → execute → repeat)          |

## Decisiones de diseño NO obvias

Estas son las cosas que parecen bugs pero son intencionales, o que fueron
resueltas a base de fix rounds durante el SDD. Si te las encuentras, lee
primero esto antes de "arreglarlas".

### 1. El runner NO sustituye `{prompts}` ni `{<step_id>}`

Solo `{i}` y `{<input_key>}` (vía env vars `PQ_INPUT_<KEY>`). Esto fue una
**decisión deliberada** durante Task 11 — el brief original tenía `{prompts}`
pero era código inventado para hacer pasar un test que también lo inventaba.
Se DROPEÓ. Los outputs entre steps se referencian por path en disco, no por
placeholder.

Si alguien pide "pero el spec dice `{imagenes}`" → revisar
`docs/superpowers/specs/2026-08-14-pipeline-queue-design.md` y la ruling del
ledger. La implementación actual cubre el caso común; cualquier placeholder
adicional necesita justificación explícita.

### 2. Skip-if-exists: tres ramas distintas

En `pq/runner.py` el skip check NO es trivial. Hay tres casos que se manejan
de forma diferente:

- `step.iterates is None` → chequea cada `step.produces` directamente contra
  disco (`(pipeline.dir / p).exists()`).
- `step.iterates.count is not None` → chequea `iteration.substituted_outputs`.
- `step.iterates.count_from is not None` → chequea `_count_from_outputs_exist`
  (cada archivo matched tiene su output derivado).

Si todas las salidas existen, devuelve `StepResult(status="skipped")`. Esto se
aplica una vez antes del bucle de iteraciones en `run_step_with_retries` (no
por iteración, para no leer disco N veces).

### 3. PID tracking para SIGKILL

`pq/cancel.py` lee `meta.get("pid")` y mata ese PID. El runner (`_run_subprocess_with_pid_tracking` en `pq/runner.py`) escribe el PID a
`<data_dir>/runs/<run_id>/meta.json` **antes** de `proc.wait()` y lo limpia
**en el finally**. Si alguien usa `subprocess.run` directo sin pasar por el
helper, el SIGKILL de `pq cancel` no funcionará — el PID nunca se persiste.

### 4. Backoff interruptible

`time.sleep(delay)` está reemplazado por `_wait_or_stop(stop, total, poll=1.0)`
en el path de retry. Esto permite que `pq cancel` o SIGINT aborte un retry
inmediatamente en vez de esperar hasta 10 minutos. NO uses `time.sleep` en
ningún path que pueda bloquear una cancelación.

### 5. Snapshot en `pq add`, no lectura directa de disco

`pq/cli.py::add_cmd` parsea el YAML, valida, escribe un snapshot a
`<data_dir>/runs/<run_id>/meta.json` con `{snapshot: <yaml_dict>, pipeline_dir}`,
y luego inserta la row en la DB. El worker reconstruye el `Pipeline` desde el
snapshot vía `_pipeline_from_snapshot` en `pq/worker.py`, **nunca** relee el
YAML de disco. Esto es intencional: el pipeline en disco puede cambiar entre
el `add` y la ejecución, pero el snapshot congela la versión.

Si el `meta.json` no tiene la clave `snapshot`, el worker hace fallback a
interpretar el meta entero como el snapshot (`meta.get("snapshot", meta)`).

### 6. El `first_iteration` flag en `worker_loop`

`worker_loop` usa `first_iteration = True` y `while not stop.should_stop or first_iteration`
para garantizar que al menos UN run se ejecuta aunque `stop.should_stop` esté
en `True` desde el principio. Esto es para que el test
`test_worker_executes_one_pipeline` (y el patrón real "process N runs then
exit") funcione. NO lo "arregles" cambiando a `while not stop.should_stop` —
romperás ese test.

### 7. Schema drift intencional

Hay columnas/tablas en el schema SQLite que se escriben pero nunca se leen:
- `step_iterations` table (creada, vacía)
- `steps.attempts` (escrito a 0 en add, nunca incrementado)
- `steps.needs_json`, `iterates_json`, `produces_json` (escritos, ignorados —
  la fuente de verdad es el snapshot)

Esto se decidió durante el final review. Migrar el schema requiere tocar
instancias existentes; se difirió. Si vas a usar estas columnas, **primero**
mira si el worker las necesita de verdad, y si no, déjalas o quítalas.

### 8. Cycle pointer es en-memoria, no en DB

`pq/worker.py::worker_loop` mantiene `cycle_idx` como variable local
Python: rota por la lista `cfg.cycle_pipelines` avanzando una posición
por cada auto-enqueue exitoso. Al rearrancar el daemon vuelve a 0. Eso
es intencional: el estado "ya despaché rot1" es efímero — lo que
sobrevive es el `cooldown_until` y el counter de uploads del día, que
viven en la DB. Tras un SIGKILL+restart del daemon, el primer ciclo
puede que re-despache el mismo pipeline que acababa de terminar, pero
su cooldown lo va a bloquear si corresponde — la rotación pura es solo
una pista de fairness, no una garantía.

NO intentes "persistir el cycle_idx en la DB" para hacerlo resistente a
restart: el cooldown ya cubre la semántica de "no re-ejecutar
inmediatamente", y un puntero persistente introduciría confusión sobre
cuál era "el siguiente" tras un crash.

### 9. El ciclo auto-encola en una iteración separada

Cuando `worker_loop` detecta que la cola está vacía pero
`cfg.cycle_pipelines` tiene un pipeline "due", llama a
`cycle_mod.enqueue_cycle_run` que escribe un run en `queued` y hace
`continue` — NO ejecuta ese run en la misma iteración. La razón:
preservar la regla FIFO absoluta del sistema. Si el ciclo ejecuta su
propio run inline, un `pq add` manual que llegó entre el `pick_next_run`
y la ejecución se saltaría sin oportunidad. Haciendo `continue`, el
siguiente loop vuelve a `pick_next_run` y deja que el motor FIFO decida.

## Cómo correr los tests

```bash
.venv/bin/pytest -v              # 55 tests, ~1.3s
.venv/bin/pytest tests/test_runner.py -v    # subset
```

Los tests usan `tmp_path`, `CliRunner`, y subprocesses reales (no mocks para
shells). Son integration tests más que unit tests — están diseñados para
detectar bugs reales.

## Documentos de referencia

- **Spec:** `docs/superpowers/specs/2026-08-14-pipeline-queue-design.md` —
  la autoridad. Si hay conflicto entre spec y código, el spec gana (pero
  registra el conflicto en el ledger).
- **Plan:** `docs/superpowers/plans/2026-08-14-pipeline-queue.md` — las 21
  tasks originales con su brief.
- **Ledger:** `.superpowers/sdd/2026-08-14-pipeline-queue/progress.md` —
  resolución completa de rulings, fix rounds, y desviaciones del brief. Si
  estás debuggeando algo y no entiendes POR QUÉ algo es como es, mira aquí
  primero.

## Convenciones del proyecto

- Type hints en todas las signatures.
- `from __future__ import annotations` en todos los módulos nuevos.
- Tests van en `tests/` con el mismo nombre que el módulo + `test_` prefix.
- Cada cambio termina con un commit (no agrupes múltiples fixes en uno).
- Mensajes de commit en inglés, formato `tipo: descripción corta`.
- YAGNI: no añadir features no pedidas. Si dudas, lee el spec.
- No subagentes a no ser que el usuario lo pida explícitamente.

## Flujo de datos típico

```
1. pq add <dir> --input k=v
   ↓
   cli.add_cmd:
     load_pipeline(dir)            # pq/pipelines.py
     validate_pipeline(pipe)       # 3-color DFS, mutual exclusion
     snapshot = parsed_yaml_dict
     write meta.json               # <data_dir>/runs/<id>/meta.json
     INSERT INTO runs              # status=queued, cooldown_until=...

2. pq daemon (loop):
   pick_next_run(conn, now, max_uploads, today)  # pq/scheduler.py
     └→ SELECT runs WHERE status IN (queued,waiting)
        AND cooldown_until IS NULL OR <= now
        AND (no upload step OR today's counter < max)
        ORDER BY id ASC LIMIT 1
   ↓
   _execute_run(conn, run_id, ...):              # pq/worker.py
     rebuild Pipeline from meta.json snapshot
     topo_order from `needs`
     for each step in topo_order:
       _run_single_iteration (or with retries via run_step_with_retries):
         Popen subprocess
         write meta.json {"pid": <pid>}
         proc.wait()
         clear meta.json pid
       update step status (done/failed/skipped)
       if upload: scheduler.mark_upload_done (counter++)
     update run status (done/failed/waiting)
     if done: apply cooldown_until = finished + cooldown_seconds

3. pq cancel <id>:
   cancel_run(conn, id, data_dir):
     read meta.json, get pid
     os.kill(pid, SIGKILL)
     UPDATE runs SET status='cancelled', finished_at=now
     UPDATE steps SET status='failed' WHERE status IN (pending,running)
```

Si vas a modificar algo, entiende primero en qué step del flujo cae y quién
lo consume después.