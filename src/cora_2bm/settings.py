"""2-BM-specific settings layered on top of CORA's base settings."""

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings2BM(BaseSettings):
    """All env vars prefixed with CORA_2BM_.

    These sit alongside CORA's own CORA_* vars in the same .env file.
    """

    model_config = SettingsConfigDict(
        env_prefix="CORA_2BM_",
        env_file=".env",
        extra="ignore",
    )

    # ── Hutch selection ────────────────────────────────────────────────────────
    hutch: str = "A"  # "A" or "B"

    # ── EPICS PV prefixes ──────────────────────────────────────────────────────
    # Rotation stages
    pv_rot_stage_a: str = "2bma:m49"       # ABS250MP-M-AS, 500 rpm
    pv_rot_stage_b: str = "2bmb:m1"        # ABRS-150MP-M-AS, 500 rpm

    # Sample stages (hutch A)
    pv_sample_top_x_a: str = "2bma:m41"
    pv_sample_top_z_a: str = "2bma:m42"
    pv_sample_y_a: str = "2bma:m44"

    # Sample stages (hutch B)
    pv_sample_top_x_b: str = "2bmb:m46"
    pv_sample_top_z_b: str = "2bmb:m47"
    pv_sample_y_b: str = "2bmb:m44"

    # Detector prefixes (AreaDetector IOC prefix)
    pv_detector_prefix_a: str = "2bma:Oryx1:"   # FLIR Oryx, hutch A
    pv_detector_prefix_b: str = "2bmb:Oryx1:"   # FLIR Oryx, hutch B

    # TomoScan EPICS IOC prefix (used by the TomoScan adapter)
    pv_tomoscan_prefix_a: str = "2bma:TomoScan:"
    pv_tomoscan_prefix_b: str = "2bmb:TomoScan:"

    # ── Data paths ─────────────────────────────────────────────────────────────
    data_root: str = "/local/data"          # fast local NVMe during acquisition
    archive_root: str = "/data/2bm"         # GPFS long-term storage
    tomopy_script: str = "tomopy"           # CLI entry point in tomopy conda env
    tomocupy_script: str = "tomocupy"       # CLI entry point in tomocupy conda env

    # ── Analysis ───────────────────────────────────────────────────────────────
    # "tomopy" | "tomocupy" — selects which backend queue_run() submits to
    recon_backend: str = "tomocupy"
    recon_extra_args: str = "--remove-stripe-method fw --rotation-axis-auto auto"
