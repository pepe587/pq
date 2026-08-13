# pipeline-queue

FIFO executor for declarative YAML pipelines.

See `docs/superpowers/specs/2026-08-14-pipeline-queue-design.md` for the full design.

## Quick start

```bash
pip install -e .
pq add examples/youtube-video --input topic="X"
pq daemon
```
