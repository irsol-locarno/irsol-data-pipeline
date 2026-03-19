# Running the Pipelines

This page gives a brief overview of how to invoke each pipeline. For full details — including step-by-step walk-throughs, output files, and Prefect deployment parameters — see the dedicated page for each pipeline.

## Available pipelines

| Pipeline | Quick invocation | Full doc |
|---|---|---|
| **Flat-field correction** | Python API — see doc | [pipeline-flat-field-correction.md](pipeline-flat-field-correction.md) |
| **Slit image generation** | Python API — see doc | [pipeline-slit-image-generation.md](pipeline-slit-image-generation.md) |
| **Prefect maintenance** | Prefect deployment only — see doc | [pipeline-maintenance.md](pipeline-maintenance.md) |

## Prefect activation environment variable

`PREFECT_ENABLED` is the only environment variable used for orchestration behavior in this repository.

- `PREFECT_ENABLED=true`: project decorators integrate with Prefect runtime metadata.
- `PREFECT_ENABLED` unset/false: the same functions remain directly callable as plain Python.

This variable controls Prefect invasiveness into execution behavior; it is not used to pass dynamic runtime parameters.

## Dynamic runtime parameters

Dynamic flow parameters are resolved in this order:

1. Value explicitly provided at run time (`--param ...` in CLI or **Custom Run** in UI).
2. Value loaded from the corresponding Prefect Variable.

Dynamic flow runtime parameters are not configured via environment variables.
Baseline dynamic values should be managed in Prefect Variables, not hardcoded in flow or deployment code.

Managed Prefect Variables are bootstrapped with:

```bash
uv run entrypoints/bootstrap_variables.py
```

Managed variable names:

| Variable | Used by flow parameter |
|---|---|
| `jsoc-email` | `jsoc_email` in slit-image flows |
| `cache-expiration-hours` | `hours` in cache-cleanup maintenance flow |
| `flow-run-expiration-hours` | `hours` in run-history cleanup maintenance flow |

Keep these variables set in Prefect before serving deployments.



## Make targets

```bash
make prefect/dashboard                             # Start the Prefect server + UI at :4200
make prefect/serve-flat-field-correction-pipeline  # Serve flat-field correction deployments
make prefect/serve-slit-image-pipeline             # Serve slit image generation deployments
make prefect/serve-maintenance-pipeline            # Serve the maintenance deployment
make prefect/reset                                 # Reset the local Prefect database (destructive)
```

For production deployment (keeping workers alive with `screen` or `systemd`), see [prefect-production.md](prefect-production.md).
