"""Helpers to launch TomoScan scan types as CORA Automation workflow steps.

Rather than wrapping TomoScan's Python API as a DeviceCommandPort (which would
give CORA per-motor control), these helpers treat a full TomoScan scan as a
single atomic workflow action.  The CORA Automation domain defines the workflow;
each step calls one of the functions below via a subprocess.

Usage in a workflow definition (pseudo-code):
    POST /api/v1/automation/workflows/
    {
      "steps": [
        {"action": "shell", "command": "tomoscan single --tomoscan-prefix 2bma:TomoScan:"},
        {"action": "shell", "command": "tomopy recon --file-name /local/data/<task_id>.h5"}
      ]
    }

For tighter integration (progress events, pause/resume), use the Python API
variant below and call it from a custom WorkflowEnginePort adapter.
"""

from __future__ import annotations

import asyncio
import logging
import shlex
from pathlib import Path

from ..settings import Settings2BM

logger = logging.getLogger(__name__)


class TomoScanRunner:
    """Thin asyncio wrapper around the tomoscan CLI.

    Each method corresponds to one tomoscan scan mode.
    Stdout/stderr are streamed to the logger so CORA's workflow engine
    can capture progress without parsing EPICS PVs directly.
    """

    def __init__(self, settings: Settings2BM | None = None) -> None:
        self._s = settings or Settings2BM()

    @property
    def _prefix(self) -> str:
        if self._s.hutch == "A":
            return self._s.pv_tomoscan_prefix_a
        return self._s.pv_tomoscan_prefix_b

    async def _run(self, *args: str) -> int:
        cmd = ["tomoscan", *args, f"--tomoscan-prefix={self._prefix}"]
        logger.info("TomoScanRunner: %s", shlex.join(cmd))
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )
        assert proc.stdout is not None
        async for line in proc.stdout:
            logger.info("[tomoscan] %s", line.decode().rstrip())
        return await proc.wait()

    async def single(self) -> None:
        rc = await self._run("single")
        if rc != 0:
            raise RuntimeError(f"tomoscan single exited with code {rc}")

    async def vertical(self, *, start: float, stop: float, step: float) -> None:
        rc = await self._run(
            "vertical",
            f"--vertical-start={start}",
            f"--vertical-stop={stop}",
            f"--vertical-step-size={step}",
        )
        if rc != 0:
            raise RuntimeError(f"tomoscan vertical exited with code {rc}")

    async def horizontal(self, *, start: float, stop: float, step: float) -> None:
        rc = await self._run(
            "horizontal",
            f"--horizontal-start={start}",
            f"--horizontal-stop={stop}",
            f"--horizontal-step-size={step}",
        )
        if rc != 0:
            raise RuntimeError(f"tomoscan horizontal exited with code {rc}")

    async def mosaic(self, *, h_start: float, h_stop: float, h_step: float,
                     v_start: float, v_stop: float, v_step: float) -> None:
        rc = await self._run(
            "mosaic",
            f"--horizontal-start={h_start}", f"--horizontal-stop={h_stop}",
            f"--horizontal-step-size={h_step}",
            f"--vertical-start={v_start}", f"--vertical-stop={v_stop}",
            f"--vertical-step-size={v_step}",
        )
        if rc != 0:
            raise RuntimeError(f"tomoscan mosaic exited with code {rc}")

    async def stream(self, output_path: Path) -> None:
        """Run TomoScanStream; data streamed to output_path via HDF5 writer."""
        rc = await self._run("stream", f"--output={output_path}")
        if rc != 0:
            raise RuntimeError(f"tomoscan stream exited with code {rc}")
