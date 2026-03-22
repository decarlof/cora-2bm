"""DataRecorderPort implementation that writes APS Data Exchange (DX) HDF5 files.

Files are written to Settings2BM.data_root during acquisition, then the
Catalogue BC is notified via TaskOutputCreated so Globus/DM can archive them.

Data Exchange layout (minimal)
-------------------------------
/exchange/data          — float32 projections  (n_proj, rows, cols)
/exchange/data_dark     — dark fields          (n_dark, rows, cols)
/exchange/data_white    — flat/white fields    (n_flat, rows, cols)
/exchange/theta         — rotation angles (degrees)
/measurement/instrument/... — metadata (populated at close time)

See https://dxfile.readthedocs.io for the full spec.
"""

from __future__ import annotations

import logging
from pathlib import Path

import h5py
import numpy as np

from cora.acquisition.domain.value_objects import DataLocation, TaskId

from ..settings import Settings2BM

logger = logging.getLogger(__name__)


class DXRecorder:
    """Implements DataRecorderPort; writes Data Exchange HDF5 to local fast storage."""

    def __init__(self, settings: Settings2BM | None = None) -> None:
        self._settings = settings or Settings2BM()
        self._handles: dict[str, h5py.File] = {}

    def _path_for(self, task_id: TaskId) -> Path:
        root = Path(self._settings.data_root)
        root.mkdir(parents=True, exist_ok=True)
        return root / f"{task_id}.h5"

    async def open_dataset(self, task_id: TaskId) -> DataLocation:
        path = self._path_for(task_id)
        f = h5py.File(path, "w")
        # Pre-create the /exchange group — datasets are grown incrementally
        f.require_group("exchange")
        f.require_group("measurement/instrument/detector")
        f.require_group("measurement/sample")
        f.attrs["default"] = "exchange"
        self._handles[str(task_id)] = f
        logger.info("DXRecorder: opened %s", path)
        return DataLocation(uri=path.as_uri(), format="hdf5/data-exchange")

    async def write_point(
        self,
        task_id: TaskId,
        point_index: int,
        setpoints: dict[str, float],
        channel_readings: list,
        stream_name: str = "primary",
        measured_conditions: dict | None = None,
    ) -> None:
        f = self._handles.get(str(task_id))
        if f is None:
            raise RuntimeError(f"Dataset for task {task_id} is not open.")

        grp = f["exchange"]

        # Rotation angle from setpoints (key matches device registration name)
        angle = setpoints.get("rot-stage", float(point_index))

        # Grow /exchange/theta
        if "theta" not in grp:
            grp.create_dataset("theta", data=np.array([angle], dtype="f4"),
                               maxshape=(None,), chunks=(64,))
        else:
            ds = grp["theta"]
            ds.resize(ds.shape[0] + 1, axis=0)
            ds[-1] = angle

        # Image data: each channel_reading with source=="detector" contributes a frame
        for reading in channel_readings:
            raw = getattr(reading, "value", None)
            if raw is None or not hasattr(raw, "shape"):
                continue
            key = "data_dark" if stream_name == "dark" else (
                "data_white" if stream_name in ("white", "flat") else "data"
            )
            if key not in grp:
                shape = (1, *raw.shape)
                grp.create_dataset(key, data=raw[np.newaxis], dtype="f4",
                                   maxshape=(None, *raw.shape),
                                   chunks=(1, *raw.shape))
            else:
                ds = grp[key]
                ds.resize(ds.shape[0] + 1, axis=0)
                ds[-1] = raw

        f.flush()

    async def close_dataset(self, task_id: TaskId) -> DataLocation:
        f = self._handles.pop(str(task_id), None)
        if f is None:
            raise RuntimeError(f"Dataset for task {task_id} was never opened.")
        path = Path(f.filename)
        self._write_metadata(f)
        f.close()
        logger.info("DXRecorder: closed %s", path)
        return DataLocation(uri=path.as_uri(), format="hdf5/data-exchange")

    def _write_metadata(self, f: h5py.File) -> None:
        """Write /measurement/instrument metadata at close time."""
        grp = f["measurement/instrument/detector"]
        # TODO: populate from EPICS PVs (pixel size, scintillator, lens, etc.)
        grp.attrs["description"] = "FLIR Oryx — 2-BM"
        grp.attrs["manufacturer"] = "FLIR"
        grp.attrs["beamline"] = "2-BM"
        grp.attrs["facility"] = "Advanced Photon Source"
