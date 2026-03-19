# Extending the System

## 1. Custom Flat-Field Matching Policy

Override `MaxDeltaPolicy.get_max_delta(...)` to customize measurement/flat-field matching windows.

```python
import datetime
from irsol_data_pipeline.core.models import MaxDeltaPolicy


class MyPolicy(MaxDeltaPolicy):
    def get_max_delta(
        self,
        wavelength: int,
        instrument: str = "",
        telescope: str = "",
    ) -> datetime.timedelta:
        if wavelength == 8542:
            return datetime.timedelta(hours=6)
        return self.default_max_delta
```

## 2. Add New Calibration Reference Data

Place additional `.npy` spectra in `src/irsol_data_pipeline/core/calibration/refdata/` or pass `refdata_dir` to `calibrate_measurement(...)`.

## 3. Tune Flat-Field Analysis

Pass a custom `spectroflat.Config` into `analyze_flatfield(...)` from `core/correction/analyzer.py`.

## 4. Add a New Output Format

1. Create `src/irsol_data_pipeline/io/<format>/exporter.py`.
2. Wire it from `pipeline/measurement_processor.py`.
3. Add suffix constant(s) in `core/config.py`.
4. Add new `ProcessedOutputKind` mapping in `pipeline/filesystem.py`.

## 5. Add a New Prefect Flow

Use project decorators from `irsol_data_pipeline.orchestration.decorators`.

```python
from irsol_data_pipeline.orchestration.decorators import flow


@flow(name="my-pipeline-full", flow_run_name="my-pipeline/full/{root}")
def my_flow(root: str) -> None:
    ...
```

Serve with an entrypoint script and `.to_deployment(name=...)`.

## 6. Naming Conventions (Current)

| Level | Examples |
|---|---|
| Flow names | `ff-correction-full`, `slit-images-daily`, `maintenance-cleanup`, `maintenance-cache-cleanup` |
| Deployment names | `flat-field-correction-full`, `slit-images-full`, `prefect-run-cleanup`, `cache-cleanup` |
| CLI trigger form | `<flow-name>/<deployment-name>` |
| Task run names | `ff-correction/process-day/{day_path.name}`, `slit-images/fetch-sdo-maps/{start_time}-{end_time}` |

## 7. Add Dynamic Runtime Parameters

1. Add variable enum in `orchestration/variables.py`.
2. Register bootstrap prompt in `entrypoints/bootstrap_variables.py`.
3. Resolve flow parameter first, then Prefect Variable fallback.

See [running.md](running.md) for the central runtime policy.
