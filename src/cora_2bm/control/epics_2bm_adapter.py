"""ControlSystemPort implementation for 2-BM EPICS devices.

Maps CORA's (device_id, channel_id) pairs to 2-BM PV names via a
registry built at startup from Settings2BM.

Channel naming convention
-------------------------
Each device is registered in CORA with channels whose IDs match
the suffix appended to the PV prefix:

    device_id="rot-stage-a"  channel_id="VAL"   → PV "2bma:m49.VAL"
    device_id="rot-stage-a"  channel_id="RBV"   → PV "2bma:m49.RBV"
    device_id="detector-a"   channel_id="Acquire" → PV "2bma:Oryx1:cam1:Acquire"

The registry dict is populated by build_pv_registry() below; extend
it as new devices are commissioned.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator
from datetime import UTC, datetime

import caproto.asyncio.client as ca

from cora.control.domain.value_objects import (
    ChannelId,
    DeviceId,
    DeviceStatus,
    Measurement,
    MeasurementBatch,
)

from ..settings import Settings2BM

logger = logging.getLogger(__name__)


def build_pv_registry(s: Settings2BM) -> dict[tuple[str, str], str]:
    """Return a mapping of (device_id, channel_id) → PV name.

    Extend this function when new devices are added to 2-BM.
    """
    rot = s.pv_rot_stage_a if s.hutch == "A" else s.pv_rot_stage_b
    det = s.pv_detector_prefix_a if s.hutch == "A" else s.pv_detector_prefix_b
    sx = s.pv_sample_top_x_a if s.hutch == "A" else s.pv_sample_top_x_b
    sz = s.pv_sample_top_z_a if s.hutch == "A" else s.pv_sample_top_z_b
    sy = s.pv_sample_y_a if s.hutch == "A" else s.pv_sample_y_b

    return {
        # Rotation stage
        ("rot-stage", "VAL"): f"{rot}.VAL",
        ("rot-stage", "RBV"): f"{rot}.RBV",
        ("rot-stage", "STOP"): f"{rot}.STOP",
        ("rot-stage", "VELO"): f"{rot}.VELO",
        # Sample stages
        ("sample-x", "VAL"): f"{sx}.VAL",
        ("sample-x", "RBV"): f"{sx}.RBV",
        ("sample-z", "VAL"): f"{sz}.VAL",
        ("sample-z", "RBV"): f"{sz}.RBV",
        ("sample-y", "VAL"): f"{sy}.VAL",
        ("sample-y", "RBV"): f"{sy}.RBV",
        # Detector (AreaDetector cam1)
        ("detector", "Acquire"):    f"{det}cam1:Acquire",
        ("detector", "AcquireTime"): f"{det}cam1:AcquireTime",
        ("detector", "NumImages"):  f"{det}cam1:NumImages",
        ("detector", "TriggerMode"): f"{det}cam1:TriggerMode",
        ("detector", "DetectorState_RBV"): f"{det}cam1:DetectorState_RBV",
        # HDF5 file writer plugin
        ("detector", "HDF1:Capture"): f"{det}HDF1:Capture",
        ("detector", "HDF1:FilePath"): f"{det}HDF1:FilePath",
        ("detector", "HDF1:FileName"): f"{det}HDF1:FileName",
        # TODO: add PSO, scintillator, shutter PVs as needed
    }


class Epics2BMAdapter:
    """Implements ControlSystemPort for 2-BM via caproto async CA client."""

    def __init__(self, settings: Settings2BM | None = None, timeout: float = 5.0) -> None:
        self._settings = settings or Settings2BM()
        self._timeout = timeout
        self._registry = build_pv_registry(self._settings)

    def _pv(self, device_id: DeviceId, channel_id: ChannelId) -> str:
        key = (str(device_id), str(channel_id))
        try:
            return self._registry[key]
        except KeyError:
            raise ValueError(
                f"No PV registered for device={device_id!r} channel={channel_id!r}. "
                "Add it to build_pv_registry()."
            )

    async def read(self, device_id: DeviceId, channel_id: ChannelId) -> Measurement:
        pv_name = self._pv(device_id, channel_id)
        pv = ca.get_pv(pv_name)
        reading = await asyncio.wait_for(pv.read(data_type="time"), timeout=self._timeout)
        value = reading.data[0]
        timestamp = datetime.fromtimestamp(reading.metadata.timestamp, tz=UTC)
        return Measurement(
            device_id=device_id,
            channel_id=channel_id,
            value=value,
            units="",           # TODO: read .EGU field
            timestamp=timestamp,
        )

    async def set(
        self,
        device_id: DeviceId,
        channel_id: ChannelId,
        value: float | int | str,
    ) -> None:
        pv_name = self._pv(device_id, channel_id)
        pv = ca.get_pv(pv_name)
        await asyncio.wait_for(pv.write(value, wait=True), timeout=self._timeout)

    async def execute(self, device_id: DeviceId, channel_id: ChannelId) -> None:
        # EXECUTE channels are typically 1-shot integers (e.g. STOP=1)
        await self.set(device_id, channel_id, 1)

    async def get_state(self, device_id: DeviceId) -> DeviceStatus:
        # TODO: map AreaDetector DetectorState or motor DMOV to DeviceStatus
        return DeviceStatus.IDLE

    async def read_batch(
        self,
        targets: list[tuple[DeviceId, ChannelId]],
    ) -> MeasurementBatch:
        measurements = await asyncio.gather(
            *[self.read(did, cid) for did, cid in targets]
        )
        return MeasurementBatch(
            readings=tuple(measurements),
            batch_timestamp=datetime.now(tz=UTC),
        )

    async def _watch_pv(
        self, device_id: DeviceId, channel_id: ChannelId
    ) -> AsyncIterator[Measurement]:
        """Yield a Measurement each time the PV value changes."""
        pv_name = self._pv(device_id, channel_id)
        pv = ca.get_pv(pv_name)
        queue: asyncio.Queue[Measurement] = asyncio.Queue()

        def _cb(reading: ca.ReadNotifyResponse, **_: object) -> None:
            value = reading.data[0]
            ts = datetime.fromtimestamp(reading.metadata.timestamp, tz=UTC)
            queue.put_nowait(
                Measurement(
                    device_id=device_id,
                    channel_id=channel_id,
                    value=value,
                    units="",
                    timestamp=ts,
                )
            )

        sub = pv.subscribe(data_type="time")
        sub.add_callback(_cb)
        try:
            while True:
                yield await queue.get()
        finally:
            sub.clear()

    def watch(self, device_id: DeviceId, channel_id: ChannelId) -> AsyncIterator[Measurement]:
        return self._watch_pv(device_id, channel_id)

    def watch_batch(
        self,
        targets: list[tuple[DeviceId, ChannelId]],
    ) -> AsyncIterator[MeasurementBatch]:
        return self._watch_batch_pvs(targets)

    async def _watch_batch_pvs(
        self,
        targets: list[tuple[DeviceId, ChannelId]],
    ) -> AsyncIterator[MeasurementBatch]:
        # Keep the latest reading for each channel; emit when any updates.
        latest: dict[tuple[str, str], Measurement] = {}
        queue: asyncio.Queue[tuple[str, str]] = asyncio.Queue()
        subscriptions = []

        for device_id, channel_id in targets:
            pv_name = self._pv(device_id, channel_id)
            pv = ca.get_pv(pv_name)
            key = (str(device_id), str(channel_id))

            def _cb(reading: ca.ReadNotifyResponse, k: tuple = key,
                    did: DeviceId = device_id, cid: ChannelId = channel_id) -> None:
                ts = datetime.fromtimestamp(reading.metadata.timestamp, tz=UTC)
                latest[k] = Measurement(
                    device_id=did, channel_id=cid,
                    value=reading.data[0], units="", timestamp=ts,
                )
                queue.put_nowait(k)

            sub = pv.subscribe(data_type="time")
            sub.add_callback(_cb)
            subscriptions.append(sub)

        try:
            while True:
                await queue.get()
                if len(latest) < len(targets):
                    continue  # wait until all channels have reported once
                readings = tuple(
                    latest[(str(did), str(cid))] for did, cid in targets
                )
                yield MeasurementBatch(
                    readings=readings,
                    batch_timestamp=datetime.now(tz=UTC),
                )
        finally:
            for sub in subscriptions:
                sub.clear()
