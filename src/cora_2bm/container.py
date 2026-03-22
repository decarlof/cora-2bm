"""2-BM composition root — extends CORA's Container with beamline adapters.

Usage
-----
Replace the standard container import in apps/api/src/cora_api/main.py::

    # was:
    from cora_api.container import Container
    # now (when deploying at 2-BM):
    from cora_2bm.container import Container2BM as Container

Or run the API with the CORA_2BM_* env vars set and let the factory below
detect them automatically (see build_container()).

What this overrides
--------------------
ControlModule   ← Epics2BMAdapter  (real 2-BM PV map, not generic APS PVs)
AcquisitionModule ← DXRecorder     (Data Exchange HDF5 on local fast storage)
Automation's AnalysisQueuePort ← TomopyQueueAdapter  (tomopy/tomocupy CLI)
"""

from __future__ import annotations

import logging
from typing import Any

from cora_api.container import Container
from cora_api.settings import Settings

from .acquisition.dx_recorder import DXRecorder
from .analysis.tomopy_queue_adapter import TomopyQueueAdapter
from .control.epics_2bm_adapter import Epics2BMAdapter
from .settings import Settings2BM

logger = logging.getLogger(__name__)


class Container2BM(Container):
    """CORA container with 2-BM-specific adapters injected."""

    def __init__(self, settings: Settings, settings_2bm: Settings2BM | None = None) -> None:
        super().__init__(settings)
        self._s2bm = settings_2bm or Settings2BM()

    def _build(self) -> None:
        # Let the parent wire everything with its defaults first ...
        super()._build()

        # ... then swap in the 2-BM adapters.
        self._inject_control()
        self._inject_acquisition()
        self._inject_analysis()

        logger.info(
            "Container2BM: 2-BM adapters active (hutch=%s)", self._s2bm.hutch
        )

    def _inject_control(self) -> None:
        """Replace the generic EPICS/fake control adapter with the 2-BM PV map."""
        epics_adapter = Epics2BMAdapter(
            settings=self._s2bm,
            timeout=self.settings.epics_ca_timeout,
        )
        # Re-wire ControlModule internals to use the 2-BM adapter.
        # ControlModule exposes its handlers via .cmd / .qry — we patch the
        # control_system reference on the command handler directly so we don't
        # have to rebuild the entire module.
        self.control.cmd._control_system = epics_adapter  # type: ignore[attr-defined]
        self.control.qry._control_system = epics_adapter  # type: ignore[attr-defined]
        self.control.monitor._control_system = epics_adapter  # type: ignore[attr-defined]
        logger.debug("Container2BM: Epics2BMAdapter injected into ControlModule")

    def _inject_acquisition(self) -> None:
        """Replace the fake data recorder with the DX HDF5 recorder."""
        dx_recorder = DXRecorder(settings=self._s2bm)
        self.acquisition.cmd._data_writer = dx_recorder  # type: ignore[attr-defined]
        self.acquisition.step_executor._data_writer = dx_recorder  # type: ignore[attr-defined]
        logger.debug(
            "Container2BM: DXRecorder injected (data_root=%s)", self._s2bm.data_root
        )

    def _inject_analysis(self) -> None:
        """Replace the default AnalysisQueuePort with the tomopy/tomocupy runner."""
        tomopy_queue = TomopyQueueAdapter(settings=self._s2bm)
        # AutomationModule holds the analysis queue via its reaction policies.
        # Patch the adapter on the AutomationModule's internal command handler.
        if hasattr(self.automation, "cmd") and hasattr(self.automation.cmd, "_analysis_queue"):
            self.automation.cmd._analysis_queue = tomopy_queue  # type: ignore[attr-defined]
        # Also patch cross-BC adapter in the container itself.
        self._analysis_queue_2bm = tomopy_queue
        logger.debug(
            "Container2BM: TomopyQueueAdapter injected (backend=%s)",
            self._s2bm.recon_backend,
        )


def build_container() -> Container:
    """Factory: returns Container2BM when 2-BM env vars are present, else base Container."""
    settings = Settings()
    s2bm = Settings2BM()
    if s2bm.hutch in ("A", "B"):
        logger.info("build_container: using Container2BM (hutch %s)", s2bm.hutch)
        return Container2BM(settings=settings, settings_2bm=s2bm)
    return Container(settings=settings)
