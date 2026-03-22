"""CLI entry point: ``cora-2bm-sync``

Mirrors the usage pattern of ``dmagic tag``:
  - reads APS credentials from ~/.scheduling_credentials
  - connects to the APS scheduling REST API (same as DMagic)
  - pushes user / proposal / session records into CORA

Usage
-----
    cora-2bm-sync                        # sync current shift
    cora-2bm-sync --set 1                # sync shift starting 1 day from now
    cora-2bm-sync --set -1               # sync the shift that ended yesterday
    cora-2bm-sync --gup 123456           # sync a specific GUP regardless of date
"""

from __future__ import annotations

import argparse
import logging
import os
import sys

from .dmagic_sync import DmagicSync


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="cora-2bm-sync",
        description="Sync 2-BM beamtime from the APS scheduling system into CORA.",
    )
    p.add_argument(
        "--cora-url",
        default=os.environ.get("CORA_API_URL", "http://localhost:8000"),
        help="CORA API base URL (default: $CORA_API_URL or http://localhost:8000)",
    )
    p.add_argument(
        "--cora-token",
        default=os.environ.get("CORA_API_TOKEN", ""),
        help="Bearer token for CORA authentication (default: $CORA_API_TOKEN)",
    )
    p.add_argument(
        "--resource-id",
        default=os.environ.get("CORA_2BM_RESOURCE_ID", "2bm"),
        help="CORA resource ID for the 2-BM beamline (default: $CORA_2BM_RESOURCE_ID or '2bm')",
    )
    # APS scheduling API args — same names/defaults as DMagic so existing
    # config files and wrapper scripts work unchanged
    p.add_argument(
        "--url",
        default="https://beam-api.aps.anl.gov",
        help="APS scheduling REST API base URL",
    )
    p.add_argument(
        "--beamline",
        default="2-BM-A,B",
        help="Beamline ID in the APS scheduling system",
    )
    p.add_argument(
        "--credentials",
        default=os.path.expanduser("~/.scheduling_credentials"),
        help="Path to APS credentials file (username|password)",
    )
    p.add_argument(
        "--set",
        type=int,
        default=0,
        dest="set",
        metavar="DAYS",
        help="Day offset from today (e.g. --set 1 = tomorrow). Same as dmagic --set.",
    )
    p.add_argument(
        "--gup",
        default=None,
        metavar="GUP_NUMBER",
        help="Sync a specific GUP number instead of the currently active proposal.",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch from APS and print what would be synced, but make no CORA API calls.",
    )
    p.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable debug logging.",
    )
    return p


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(levelname)s %(name)s: %(message)s",
    )
    log = logging.getLogger("cora-2bm-sync")

    from dmagic import authorize, scheduling as sched

    auth = authorize.basic(args.credentials)
    if auth is None:
        log.error("Could not load APS credentials from %s", args.credentials)
        return 1

    # Determine run and beamtime
    run = sched.current_run(auth, args)
    if run is None:
        log.error("Could not determine current APS run")
        return 1
    log.info("APS run: %s", run)

    if args.gup:
        beamtime = sched.get_beamtime(args.gup, auth, args)
        if beamtime is None:
            log.error("GUP %s not found in run %s", args.gup, run)
            return 1
    else:
        proposals = sched.beamtime_requests(run, auth, args)
        if not proposals:
            log.error("No proposals found for run %s / beamline %s", run, args.beamline)
            return 1
        beamtime = sched.get_current_proposal(proposals, args)
        if beamtime is None:
            log.error("No active proposal right now. Use --set to offset the date.")
            return 1

    gup_id = str(beamtime["beamtime"]["proposal"]["gupId"])
    title  = beamtime["beamtime"]["proposal"].get("proposalTitle", "")
    log.info("Found GUP %s: %s", gup_id, title)

    if args.dry_run:
        _print_dry_run(beamtime, run)
        return 0

    sync = DmagicSync(
        cora_url=args.cora_url,
        cora_token=args.cora_token,
        resource_id=args.resource_id,
    )
    result = sync.sync_beamtime(beamtime, run)
    print(result)
    return 0


def _print_dry_run(beamtime: dict, run: str) -> None:
    """Print a summary of what would be synced without calling CORA."""
    prop = beamtime["beamtime"]["proposal"]
    bt   = beamtime["beamtime"]
    print("=== DRY RUN — no CORA API calls made ===")
    print(f"GUP         : {prop['gupId']}")
    print(f"Title       : {prop.get('proposalTitle', '')}")
    print(f"Run/cycle   : {run}")
    print(f"Type        : {prop.get('proposalType', {}).get('display', 'GUP')}")
    print(f"Proprietary : {prop.get('proprietaryFlag', 'N')}")
    print(f"Mail-in     : {prop.get('mailInFlag', 'N')}")
    print(f"Submitted   : {prop.get('submittedDate', '')}")
    print(f"Granted shifts  : {bt.get('grantedShifts')}  ({(bt.get('grantedShifts') or 0) * 8} h)")
    print(f"Scheduled shifts: {bt.get('scheduledShifts')}  ({(bt.get('scheduledShifts') or 0) * 8} h)")
    print(f"Start       : {beamtime['startTime']}")
    print(f"End         : {beamtime['endTime']}")
    print("Experimenters:")
    for exp in prop.get("experimenters", []):
        pi = " [PI]" if exp.get("piFlag") == "Y" else ""
        print(f"  {exp.get('firstName')} {exp.get('lastName')}{pi}"
              f"  <{exp.get('email', '')}>"
              f"  badge={exp.get('badge', '')}  {exp.get('institution', '')}")


if __name__ == "__main__":
    sys.exit(main())
