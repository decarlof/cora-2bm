"""AnalysisQueuePort implementation that submits tomopy / tomocupy jobs.

Runs the reconstruction CLI as a subprocess so it executes in the correct
conda environment (tomopy or tomocupy), independent of the CORA API process.

CORA calls queue_run(pipeline_id, dataset_ref, correlation_id) when
TaskOutputCreated fires (driven by AutoAnalysisPolicy in the Automation BC).

pipeline_id values understood by this adapter
---------------------------------------------
"tomopy"    → tomopy recon --file-name <path>
"tomocupy"  → tomocupy recon --file-name <path>

Any other pipeline_id raises ValueError so CORA surfaces it cleanly.
"""

from __future__ import annotations

import asyncio
import logging
import shlex
from pathlib import Path
from urllib.parse import urlparse

from ..settings import Settings2BM

logger = logging.getLogger(__name__)


class TomopyQueueAdapter:
    """Implements AnalysisQueuePort; submits reconstruction jobs as subprocesses."""

    def __init__(self, settings: Settings2BM | None = None) -> None:
        self._s = settings or Settings2BM()

    async def queue_run(
        self,
        pipeline_id: str,
        dataset_ref: str,
        correlation_id: str = "",
    ) -> str:
        """Launch a reconstruction job and return a run ID (the process PID as string)."""
        path = self._resolve_path(dataset_ref)
        cmd = self._build_cmd(pipeline_id, path)
        logger.info(
            "TomopyQueueAdapter: submitting %s job for %s (correlation=%s)",
            pipeline_id, path, correlation_id,
        )
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )
        # Fire-and-forget: log output in a background task
        asyncio.create_task(self._stream_logs(proc, pipeline_id, path))
        run_id = str(proc.pid)
        logger.info("TomopyQueueAdapter: started PID %s", run_id)
        return run_id

    def _resolve_path(self, dataset_ref: str) -> Path:
        """Convert a DataLocation URI or plain path to a local Path."""
        if dataset_ref.startswith("file://"):
            return Path(urlparse(dataset_ref).path)
        return Path(dataset_ref)

    def _build_cmd(self, pipeline_id: str, path: Path) -> list[str]:
        extra = shlex.split(self._s.recon_extra_args)
        if pipeline_id in ("tomocupy", "tomocupy-recon"):
            return [self._s.tomocupy_script, "recon", "--file-name", str(path), *extra]
        if pipeline_id in ("tomopy", "tomopy-recon"):
            return [self._s.tomopy_script, "recon", "--file-name", str(path), *extra]
        raise ValueError(
            f"Unknown pipeline_id {pipeline_id!r}. "
            "Expected 'tomopy' or 'tomocupy'."
        )

    async def _stream_logs(
        self, proc: asyncio.subprocess.Process, label: str, path: Path
    ) -> None:
        assert proc.stdout is not None
        async for line in proc.stdout:
            logger.info("[%s|%s] %s", label, path.name, line.decode().rstrip())
        rc = await proc.wait()
        if rc == 0:
            logger.info("TomopyQueueAdapter: %s finished OK for %s", label, path.name)
        else:
            logger.error(
                "TomopyQueueAdapter: %s exited %d for %s", label, rc, path.name
            )
