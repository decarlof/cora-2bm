# cora-2bm

2-BM tomography beamline adapters for [CORA](../cora-decarlof), bridging
CORA's facility-neutral ports to 2-BM's existing tools at the Advanced Photon Source.

## What lives here

| Module | CORA port it implements | 2-BM tool |
|--------|------------------------|-----------|
| `control/epics_2bm_adapter.py` | `ControlSystemPort` | EPICS PVs via caproto |
| `acquisition/dx_recorder.py` | `DataRecorderPort` | Data Exchange HDF5 on local NVMe |
| `acquisition/tomoscan_workflow.py` | Automation workflow steps | TomoScan CLI |
| `analysis/tomopy_queue_adapter.py` | `AnalysisQueuePort` | tomopy / tomocupy CLI |
| `container.py` | Composition root | Wires adapters into CORA's Container |
| `settings.py` | — | `CORA_2BM_*` env vars |

## Setup

```bash
# 1. Copy env file and fill in values
cp .env.example .env

# 2. Install into the tomo-bits conda env (which has tomoscan, caproto, h5py)
conda activate tomo-bits-decarlof
pip install -e ".[dev]"

# 3. Run the CORA API with 2-BM adapters
CORA_2BM_HUTCH=A uvicorn cora_2bm.main:app --reload
```

## How the container is wired

`Container2BM` subclasses CORA's `Container` and calls `super()._build()` first,
then swaps in the 2-BM adapters:

```
Container._build()
    └─ ControlModule(fake adapter)        ← replaced by Epics2BMAdapter
    └─ AcquisitionModule(fake recorder)   ← replaced by DXRecorder
    └─ AutomationModule(fake queue)       ← replaced by TomopyQueueAdapter
```

No files inside `cora-decarlof` are modified.

## Adding a new device

1. Add PV entries to `build_pv_registry()` in `control/epics_2bm_adapter.py`
2. Add the corresponding env var to `settings.py` and `.env.example`
3. Register the device via the CORA REST API:
   ```bash
   curl -X POST http://localhost:8000/api/v1/control/devices/ \
     -d '{"device_id": "my-new-motor", "name": "New Motor", ...}'
   ```

## Running a TomoScan scan from CORA

TomoScan scans are exposed as **workflow steps** (not individual device moves).
Define a workflow via the Automation API, then invoke it:

```bash
# Define
curl -X POST http://localhost:8000/api/v1/automation/workflows/ \
  -d '{"name": "single-tomo", "steps": [{"type": "tomoscan", "mode": "single"}]}'

# Invoke
curl -X POST http://localhost:8000/api/v1/automation/workflows/{id}/invoke
```

## Reconstruction

When `CORA_AUTO_ANALYSIS_PIPELINE_ID=tomocupy` is set, CORA automatically
queues a `tomocupy recon` job after each scan completes.  The pipeline ID
is passed to `TomopyQueueAdapter.queue_run()` which selects the right CLI.
