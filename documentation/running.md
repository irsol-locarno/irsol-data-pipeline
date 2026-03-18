# Running the Pipelines

This page gives a brief overview of how to invoke each pipeline. For full details — including step-by-step walk-throughs, output files, and Prefect deployment parameters — see the dedicated page for each pipeline.

## Available pipelines

| Pipeline | Quick invocation | Full doc |
|---|---|---|
| **Flat-field correction** | Python API — see doc | [pipeline-flat-field-correction.md](pipeline-flat-field-correction.md) |
| **Slit image generation** | Python API — see doc | [pipeline-slit-image-generation.md](pipeline-slit-image-generation.md) |
| **Prefect maintenance** | Prefect deployment only — see doc | [pipeline-maintenance.md](pipeline-maintenance.md) |

## The `PREFECT_ENABLED` environment variable

All `@task` and `@flow` decorators in this codebase are conditional no-ops unless `PREFECT_ENABLED=true`. When unset, every pipeline runs as plain Python with no Prefect server required. The `make prefect/serve-*` targets set this automatically.

## Make targets

```bash
make prefect/dashboard                             # Start the Prefect server + UI at :4200
make prefect/serve-flat-field-correction-pipeline  # Serve flat-field correction deployments
make prefect/serve-slit-image-pipeline             # Serve slit image generation deployments
make prefect/serve-maintenance-pipeline            # Serve the maintenance deployment
make prefect/reset                                 # Reset the local Prefect database (destructive)
```

For production deployment (keeping workers alive with `screen` or `systemd`), see [prefect-production.md](prefect-production.md).
