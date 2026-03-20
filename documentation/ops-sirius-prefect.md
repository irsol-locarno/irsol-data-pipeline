# OPS Runbook: Sirius Prefect Operations

This runbook is the operational reference for running `irsol-data-pipeline` on
server `sirius` with a dedicated service account.

Use this page when onboarding operators, setting up long-lived services, and
performing routine health checks or incident response.

## Scope

This guide covers:

- service-account model on `sirius`;
- process topology for Prefect server plus deployment serving;
- bootstrap and lifecycle commands;
- health verification and troubleshooting checks;
- safe restart and upgrade workflow.

For pipeline behavior and parameters, keep using:

- [pipeline.md](pipeline.md)
- [running.md](running.md)
- [prefect-production.md](prefect-production.md)

## Installation Policy on `sirius`

Production operations on `sirius` should use a `uv tool` installation of
`irsol-data-pipeline` under the dedicated service user.

Canonical install command:

```bash
uv tool install irsol-data-pipeline
```

Canonical upgrade command:

```bash
uv tool upgrade irsol-data-pipeline
```

Rationale:

- keeps `idp` isolated from development virtual environments;
- simplifies operational upgrades and rollback strategy;
- reduces coupling to mutable source checkouts.

A project-specific virtual environment is for development/debug tasks only.

## 1. Operating Model on `sirius`

Run the full stack under one dedicated Unix user (example: `irsol-prefect`).

Why:

- isolates permissions from personal accounts;
- keeps logs/process ownership consistent;
- simplifies service restart and auditing.

Recommended ownership model:

- code checkout or installed environment owned by `irsol-prefect`;
- dataset tree readable/writable by `irsol-prefect` where required;
- all long-lived processes started by systemd units under `irsol-prefect`.

## 2. Process Topology

Run exactly four long-lived processes:

1. `idp prefect start`
2. `idp prefect flows serve flat-field-correction`
3. `idp prefect flows serve slit-images`
4. `idp prefect flows serve maintenance`

This is the intended production model for this repository (fault isolation and
independent restarts per pipeline domain).

## 3. One-Time Bootstrap on `sirius`

1. Install `idp` as a `uv tool` under the dedicated user:

```bash
uv tool install irsol-data-pipeline
```

2. Verify CLI availability:

```bash
idp --version
```

3. Configure Prefect API settings once:

```bash
uvx prefect config set PREFECT_API_URL=http://127.0.0.1:4200/api
uvx prefect config set PREFECT_SERVER_ANALYTICS_ENABLED=false
```

4. Configure required Prefect Variables:

```bash
idp prefect variables configure
```

Required values to set:

- `data-root-path`: dataset root on `sirius`;
- `jsoc-email`: email registered with JSOC.

Optional but recommended:

- `cache-expiration-hours` (default 672);
- `flow-run-expiration-hours` (default 672).

5. Verify baseline metadata:

```bash
idp info
idp prefect variables list
idp prefect flows list
```

## 4. systemd Service Layout

Use one unit per long-lived process. The names below are examples and can be
adapted to your local naming convention.

- `irsol-prefect-server.service`
- `irsol-prefect-serve-flatfield.service`
- `irsol-prefect-serve-slitimages.service`
- `irsol-prefect-serve-maintenance.service`

Suggested unit design:

- `User=irsol-prefect`
- `WorkingDirectory=/srv/irsol-data-pipeline` (example)
- `Restart=always`
- `RestartSec=5`
- `Environment=PREFECT_ENABLED=true`
- `ExecStart` uses `idp ...` commands listed in section 2.

After creating units:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now irsol-prefect-server.service
sudo systemctl enable --now irsol-prefect-serve-flatfield.service
sudo systemctl enable --now irsol-prefect-serve-slitimages.service
sudo systemctl enable --now irsol-prefect-serve-maintenance.service
```

## 5. Daily Operations Checklist

Run as `irsol-prefect` unless root access is required for systemd inspection.

1. Check all services are active:

```bash
systemctl status irsol-prefect-server.service
systemctl status irsol-prefect-serve-flatfield.service
systemctl status irsol-prefect-serve-slitimages.service
systemctl status irsol-prefect-serve-maintenance.service
```

2. Check Prefect API health:

```bash
curl -fsS http://127.0.0.1:4200/api/health
```

3. Check runtime metadata quickly:

```bash
idp info
```

4. Open dashboard and inspect state:

- `http://sirius:4200/deployments`
- `http://sirius:4200/runs`
- `http://sirius:4200/artifacts`

5. Confirm expected artifacts in dataset:

- flat-field outputs: `processed/*_corrected.fits`;
- slit outputs: `processed/*_slit_preview.png`;
- investigate matching `*_error.json` files.

## 6. Manual Operations

Manual trigger examples (for controlled re-runs):

```bash
uvx prefect deployment run 'ff-correction-full/flat-field-correction-full'
uvx prefect deployment run \
  'ff-correction-daily/flat-field-correction-daily' \
  --param day_path=/data/2025/20250312

uvx prefect deployment run 'slit-images-full/slit-images-full'
uvx prefect deployment run \
  'slit-images-daily/slit-images-daily' \
  --param day_path=/data/2025/20250312

uvx prefect deployment run 'maintenance-cleanup/prefect-run-cleanup'
uvx prefect deployment run 'maintenance-cache-cleanup/cache-cleanup'
```

List deployments if in doubt:

```bash
idp prefect flows list
```

## 7. Restart and Recovery Procedures

### Service restart (safe)

If only one pipeline group is failing, restart only the corresponding serve unit.

```bash
sudo systemctl restart irsol-prefect-serve-flatfield.service
sudo systemctl restart irsol-prefect-serve-slitimages.service
sudo systemctl restart irsol-prefect-serve-maintenance.service
```

If the API is unhealthy, restart the server first, then serve units.

```bash
sudo systemctl restart irsol-prefect-server.service
sudo systemctl restart irsol-prefect-serve-flatfield.service
sudo systemctl restart irsol-prefect-serve-slitimages.service
sudo systemctl restart irsol-prefect-serve-maintenance.service
```

### Reprocessing strategy

Processing is idempotent by output-file presence.

- To reprocess a measurement, remove its generated output(s) in `processed/`.
- Re-run the relevant deployment (daily flow preferred for targeted reruns).

Do not mass-delete cache/output folders unless this is an intentional recovery
action with operator sign-off.

### Prefect database reset (destructive)

Use only if run-history state is irrecoverable or intentionally purged.

```bash
idp prefect reset-database
```

Then restart all four services.

## 8. Logs and Diagnostics

Primary sources:

- systemd journal for each unit;
- pipeline file log: `solar_pipeline.log` (default current working directory);
- Prefect dashboard logs for flow/task runs.

Useful commands:

```bash
journalctl -u irsol-prefect-server.service -n 200 --no-pager
journalctl -u irsol-prefect-serve-flatfield.service -n 200 --no-pager
journalctl -u irsol-prefect-serve-slitimages.service -n 200 --no-pager
journalctl -u irsol-prefect-serve-maintenance.service -n 200 --no-pager
```

Operator triage pattern:

1. Confirm API health endpoint responds.
2. Confirm all serve units are active.
3. Inspect failed runs in dashboard.
4. Correlate with journal and `solar_pipeline.log` entries.
5. Re-run only impacted scope (single day or maintenance flow).

## 9. Change Management and Upgrades

For code updates on `sirius`:

1. Stop serve units (keep server up unless upgrading Prefect itself).
2. Upgrade runtime package (`uv tool upgrade irsol-data-pipeline`) and apply any
  related deployment changes.
3. Run validation checks (`make lint`, `make test`) before enabling schedule load.
4. Start serve units again.
5. Trigger one manual smoke run (`ff-correction-daily` recommended).
6. Verify run completion and artifact outputs before leaving scheduled mode.

## 10. Minimum Onboarding Checklist for New OPS Engineer

1. Access as `irsol-prefect` on `sirius`.
2. Run `idp info` and interpret variable status.
3. Run `idp prefect flows list` and identify six deployments.
4. Open Prefect UI and locate deployments, runs, and artifacts pages.
5. Restart one serve unit and confirm it returns to healthy state.
6. Trigger one manual daily run and verify output files.
7. Read [running.md](running.md) for parameter-level operation.
