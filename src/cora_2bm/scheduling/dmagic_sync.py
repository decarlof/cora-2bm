"""Bridge DMagic scheduling data into CORA.

This module uses DMagic as a library — it calls dmagic.scheduling functions
directly rather than duplicating any API logic.  The job of this module is
purely translation: APS beamtime dict → CORA REST calls.

Typical call sequence
---------------------
    from cora_2bm.scheduling.dmagic_sync import DmagicSync

    sync = DmagicSync(cora_url="http://localhost:8000", cora_token="...")
    result = sync.sync_current(args, credentials_file="~/.scheduling_credentials")
    print(result)  # SyncResult with user_ids, proposal_id, session_id

The sync is **idempotent**: running it twice for the same GUP does not create
duplicate records.  Users and proposals are looked up by email / GUP number
before creation is attempted.

Fields populated from DMagic (current version)
-----------------------------------------------
User:
    given_name, family_name, email, institution (profile), badge (stored
    as external identity key in special_requirements)

Proposal:
    title, team (PI + all co-investigators), run/cycle, access_path
    (derived from proposalType + proprietaryFlag), access_mode_preference
    (from mailInFlag), requested_hours (scheduledShifts × 8),
    awarded_hours (grantedShifts × 8), GUP number stored in
    special_requirements block so it can be retrieved later.

Fields pending APS REST API additions
--------------------------------------
    abstract, orcid_id, totalShiftsRequested, DOI
    — placeholders are left in the code; slot them in once available.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from typing import Any

import requests

log = logging.getLogger(__name__)

_SHIFTS_TO_HOURS = 8.0  # 1 APS shift = 8 hours


# ── Result dataclass ──────────────────────────────────────────────────────────

@dataclass
class SyncResult:
    """Identifiers of every CORA record created or found during a sync."""
    gup_id: str
    proposal_id: str           # CORA UUID
    session_id: str            # CORA UUID
    user_ids: dict[str, str]   # badge → CORA user_id
    block_id: str              # CORA allocation block UUID
    created: list[str] = field(default_factory=list)   # what was newly created
    found: list[str] = field(default_factory=list)     # what already existed

    def __str__(self) -> str:
        lines = [
            f"GUP {self.gup_id}",
            f"  proposal : {self.proposal_id}  ({'created' if 'proposal' in self.created else 'found'})",
            f"  session  : {self.session_id}  ({'created' if 'session' in self.created else 'found'})",
            f"  block    : {self.block_id}  ({'created' if 'block' in self.created else 'found'})",
            "  users:",
        ]
        for badge, uid in self.user_ids.items():
            lines.append(f"    badge {badge} → {uid}")
        return "\n".join(lines)


# ── Main class ────────────────────────────────────────────────────────────────

class DmagicSync:
    """Translates a DMagic beamtime record into CORA REST API calls.

    Parameters
    ----------
    cora_url:
        Base URL of the running CORA API, e.g. ``http://localhost:8000``.
    cora_token:
        Bearer token for CORA authentication.  Leave empty when CORA_JWT_SECRET
        is unset (dev mode).
    resource_id:
        The CORA resource ID for the 2-BM beamline.  Must be pre-registered in
        CORA before syncing.  Configurable via ``CORA_2BM_RESOURCE_ID``.
    facility_id:
        CORA facility ID (default ``"aps"``).  Used for approval sync.
    timeout:
        HTTP request timeout in seconds.
    """

    def __init__(
        self,
        cora_url: str,
        cora_token: str = "",
        resource_id: str = "2bm",
        facility_id: str = "aps",
        timeout: float = 30.0,
    ) -> None:
        self._base = cora_url.rstrip("/")
        self._resource_id = resource_id
        self._facility_id = facility_id
        self._timeout = timeout
        self._session = requests.Session()
        if cora_token:
            self._session.headers["Authorization"] = f"Bearer {cora_token}"
        self._session.headers["Content-Type"] = "application/json"

    # ── Public entry points ───────────────────────────────────────────────────

    def sync_current(self, args: Any, credentials_file: str = "~/.scheduling_credentials") -> SyncResult:
        """Sync the currently active beamtime at 2-BM into CORA.

        Uses the same ``args`` namespace as DMagic CLI commands (url, beamline,
        set offset).  Typically called at the start of a user shift.

        Parameters
        ----------
        args:
            argparse Namespace with at minimum: ``args.url``, ``args.beamline``,
            ``args.set`` (day offset, default 0).
        credentials_file:
            Path to the APS scheduling credentials file (``username|password``).
        """
        from dmagic import authorize, scheduling

        auth = authorize.basic(credentials_file)
        if auth is None:
            raise RuntimeError(f"Could not read APS credentials from {credentials_file}")

        run = scheduling.current_run(auth, args)
        if run is None:
            raise RuntimeError("Could not determine current APS run from the scheduling API")

        proposals = scheduling.beamtime_requests(run, auth, args)
        if not proposals:
            raise RuntimeError(f"No beamtime requests found for run {run} on beamline {args.beamline}")

        proposal = scheduling.get_current_proposal(proposals, args)
        if proposal is None:
            raise RuntimeError(
                f"No active proposal found right now for run {run}. "
                "Use args.set to offset the date if checking a past/future shift."
            )

        return self.sync_beamtime(proposal, run)

    def sync_beamtime(self, beamtime: dict, run_name: str) -> SyncResult:
        """Sync a specific beamtime dict (as returned by DMagic) into CORA.

        Parameters
        ----------
        beamtime:
            Raw beamtime dict from ``scheduling.beamtime_requests()`` or
            ``scheduling.get_beamtime()``.
        run_name:
            APS run name, e.g. ``"2024-1"``.  Used as the CORA cycle.
        """
        fields = self._extract(beamtime, run_name)
        log.info("Syncing GUP %s (%s) into CORA", fields["gup_id"], fields["title"])

        result = SyncResult(
            gup_id=fields["gup_id"],
            proposal_id="",
            session_id="",
            block_id="",
            user_ids={},
        )

        # Step 1 — ensure every experimenter has a CORA user account
        result.user_ids = self._sync_users(fields["experimenters"], result)

        # Step 2 — build the CORA team list (PI first, then co-investigators)
        team = self._build_team(fields["experimenters"], result.user_ids)

        # Step 3 — ensure the proposal exists in CORA
        proposal_id = self._find_proposal_by_gup(fields["gup_id"])
        if proposal_id:
            log.info("Proposal for GUP %s already in CORA: %s", fields["gup_id"], proposal_id)
            result.found.append("proposal")
        else:
            proposal_id = self._create_proposal(fields, team)
            log.info("Created CORA proposal %s for GUP %s", proposal_id, fields["gup_id"])
            result.created.append("proposal")
            # Drive proposal through the CORA workflow: PLANNED → SUBMITTED → APPROVED
            self._submit_proposal(proposal_id)
            self._accept_proposal(
                proposal_id,
                cycle=fields["run_name"],
                awarded_hours=fields["awarded_hours"],
            )
        result.proposal_id = proposal_id

        # Step 4 — create the allocation block (beamtime window)
        block_id = self._find_block_by_proposal(proposal_id)
        if block_id:
            log.info("Allocation block already exists for proposal %s", proposal_id)
            result.found.append("block")
        else:
            block_id = self._create_block(
                proposal_id=proposal_id,
                start=fields["start_time"],
                end=fields["end_time"],
                cycle=fields["run_name"],
            )
            log.info("Created allocation block %s", block_id)
            result.created.append("block")
        result.block_id = block_id

        # Step 5 — create the experiment session
        session_id = self._find_session_by_proposal(proposal_id)
        if session_id:
            log.info("Session already exists for proposal %s: %s", proposal_id, session_id)
            result.found.append("session")
        else:
            session_id = self._create_session(proposal_id, fields)
            log.info("Created session %s", session_id)
            result.created.append("session")
            self._add_team_to_session(session_id, team)
            self._start_session(session_id)
        result.session_id = session_id

        # Step 6 — sync safety approvals for every team member
        for badge, user_id in result.user_ids.items():
            self._sync_approval(user_id, proposal_id, session_id)

        return result

    # ── Data extraction ───────────────────────────────────────────────────────

    def _extract(self, beamtime: dict, run_name: str) -> dict:
        """Pull all needed fields out of the raw DMagic beamtime dict."""
        prop = beamtime["beamtime"]["proposal"]
        bt   = beamtime["beamtime"]

        gup_id        = str(prop["gupId"])
        title         = prop.get("proposalTitle", f"GUP-{gup_id}")
        experimenters = prop.get("experimenters", [])

        # Proposal type → CORA access_path
        prop_type    = prop.get("proposalType", {}).get("display", "GUP")
        proprietary  = str(prop.get("proprietaryFlag", "N")).upper() == "Y"
        access_path  = self._map_access_path(prop_type, proprietary)

        # Shifts → hours  (totalShiftsRequested is null in current API)
        granted_shifts   = bt.get("grantedShifts")   or 0
        scheduled_shifts = bt.get("scheduledShifts")  or 0
        awarded_hours    = float(granted_shifts)   * _SHIFTS_TO_HOURS
        requested_hours  = float(scheduled_shifts) * _SHIFTS_TO_HOURS
        if requested_hours == 0:
            requested_hours = awarded_hours  # fallback if scheduled is missing

        # Mail-in → access mode preference
        mail_in = str(prop.get("mailInFlag", "N")).upper() == "Y"
        access_mode = "MAIL_IN" if mail_in else ""

        # Submitted date (not a CORA field yet — kept in special_requirements)
        submitted_date = prop.get("submittedDate", "")
        if submitted_date:
            # Normalise to YYYY-MM-DD
            try:
                import datetime as _dt
                submitted_date = _dt.datetime.fromisoformat(submitted_date).strftime("%Y-%m-%d")
            except ValueError:
                pass

        # APS-specific block stored as structured text in special_requirements
        special = (
            f"gup_id={gup_id}\n"
            f"prop_type={prop_type}\n"
            f"proprietary={'Y' if proprietary else 'N'}\n"
            f"mail_in={'Y' if mail_in else 'N'}\n"
            f"submitted_date={submitted_date}\n"
            f"run={run_name}\n"
            # placeholder for fields pending APS API additions:
            # f"orcid=...\n"
            # f"abstract=...\n"
            # f"doi=...\n"
        )

        return {
            "gup_id":          gup_id,
            "title":           title,
            "experimenters":   experimenters,
            "access_path":     access_path,
            "access_mode":     access_mode,
            "awarded_hours":   awarded_hours,
            "requested_hours": requested_hours,
            "run_name":        run_name,
            "start_time":      beamtime["startTime"],
            "end_time":        beamtime["endTime"],
            "special":         special,
        }

    @staticmethod
    def _map_access_path(prop_type: str, proprietary: bool) -> str:
        """Map APS proposal type + proprietary flag to a CORA access_path."""
        if proprietary:
            return "PROPRIETARY"
        mapping = {
            "PUP": "PARTNER",
            "GUP": "STANDARD",
        }
        return mapping.get(prop_type.upper(), "STANDARD")

    # ── User sync ─────────────────────────────────────────────────────────────

    def _sync_users(self, experimenters: list[dict], result: SyncResult) -> dict[str, str]:
        """Ensure every experimenter has a CORA user account. Returns badge→user_id."""
        badge_to_uid: dict[str, str] = {}
        for exp in experimenters:
            badge = str(exp.get("badge", "")).strip()
            email = str(exp.get("email", "")).strip().lower()
            if not email:
                log.warning("Experimenter %s %s has no email — skipping",
                            exp.get("firstName", ""), exp.get("lastName", ""))
                continue

            user_id = self._find_user_by_email(email)
            if user_id:
                log.debug("User %s already in CORA: %s", email, user_id)
                result.found.append(f"user:{email}")
            else:
                user_id = self._create_user(exp)
                log.info("Created CORA user %s (%s)", email, user_id)
                result.created.append(f"user:{email}")

            # Always update profile (institution may have changed)
            self._update_profile(user_id, exp)

            badge_to_uid[badge] = user_id

        return badge_to_uid

    def _find_user_by_email(self, email: str) -> str | None:
        """Return the CORA user_id for this email, or None if not found."""
        resp = self._get("/api/v1/identity/users/")
        for user in resp:
            if str(user.get("email", "")).lower() == email:
                return str(user["user_id"])
        return None

    def _create_user(self, exp: dict) -> str:
        email     = str(exp.get("email", "")).strip().lower()
        badge     = str(exp.get("badge", "")).strip()
        username  = self._make_username(email, badge)
        payload   = {
            "username":    username,
            "email":       email,
            "given_name":  exp.get("firstName", ""),
            "family_name": exp.get("lastName", ""),
            "role":        "USER",
            "status":      "ACTIVE",
        }
        resp = self._post("/api/v1/identity/users/", payload)
        return resp["user_id"]

    def _update_profile(self, user_id: str, exp: dict) -> None:
        payload = {
            "institution": exp.get("institution", ""),
            # orcid_id: pending APS API addition
        }
        self._patch(f"/api/v1/identity/users/{user_id}/profile", payload)

    @staticmethod
    def _make_username(email: str, badge: str) -> str:
        """Derive a unique CORA username from email and APS badge number."""
        prefix = email.split("@")[0]
        prefix = re.sub(r"[^a-z0-9_.-]", "", prefix.lower())
        return f"{prefix}_{badge}" if badge else prefix

    # ── Proposal sync ─────────────────────────────────────────────────────────

    def _find_proposal_by_gup(self, gup_id: str) -> str | None:
        """Return CORA proposal_id for this GUP, or None if not yet synced."""
        resp = self._get("/api/v1/scheduling/proposals")
        for p in resp:
            sr = p.get("special_requirements", "") or ""
            if f"gup_id={gup_id}" in sr:
                return str(p["proposal_id"])
        return None

    def _build_team(
        self, experimenters: list[dict], badge_to_uid: dict[str, str]
    ) -> list[dict]:
        """Build the CORA team list from experimenters, PI first."""
        team = []
        for exp in experimenters:
            badge   = str(exp.get("badge", "")).strip()
            user_id = badge_to_uid.get(badge)
            if not user_id:
                continue
            role = "PI" if exp.get("piFlag") == "Y" else "CO_I"
            team.append({"user_id": user_id, "role": role})
        # Guarantee PI is first (CORA requires exactly one PI in the team)
        team.sort(key=lambda m: 0 if m["role"] == "PI" else 1)
        return team

    def _create_proposal(self, fields: dict, team: list[dict]) -> str:
        payload = {
            "title":                  fields["title"],
            "resource_id":            self._resource_id,
            "requested_hours":        fields["requested_hours"],
            "abstract":               "",  # pending APS API addition
            "workload_class":         "BATCH",
            "access_path":            fields["access_path"],
            "team":                   team,
            "guest_collaborators":    [],
            "requested_cycles":       [fields["run_name"]],
            "access_mode_preference": fields["access_mode"],
            "special_requirements":   fields["special"],
            "technique_ids":          ["tomography"],
        }
        resp = self._post("/api/v1/scheduling/proposals", payload)
        return resp["proposal_id"]

    def _submit_proposal(self, proposal_id: str) -> None:
        self._post(f"/api/v1/scheduling/proposals/{proposal_id}/submit", {})

    def _accept_proposal(self, proposal_id: str, cycle: str, awarded_hours: float) -> None:
        self._post(f"/api/v1/scheduling/proposals/{proposal_id}/accept", {
            "score":         5,            # APS already approved it externally
            "cycle":         cycle,
            "awarded_hours": awarded_hours or None,
        })

    # ── Allocation block ──────────────────────────────────────────────────────

    def _find_block_by_proposal(self, proposal_id: str) -> str | None:
        resp = self._get("/api/v1/scheduling/blocks")
        for b in resp:
            if str(b.get("proposal_id", "")) == proposal_id:
                return str(b["block_id"])
        return None

    def _create_block(
        self, proposal_id: str, start: str, end: str, cycle: str
    ) -> str:
        resp = self._post("/api/v1/scheduling/blocks", {
            "proposal_id": proposal_id,
            "resource_id": self._resource_id,
            "start":       start,
            "end":         end,
            "cycle":       cycle,
        })
        return resp["block_id"]

    # ── Session ───────────────────────────────────────────────────────────────

    def _find_session_by_proposal(self, proposal_id: str) -> str | None:
        resp = self._get("/api/v1/experiment/sessions/")
        for s in resp:
            if str(s.get("proposal_id", "")) == proposal_id:
                return str(s["session_id"])
        return None

    def _create_session(self, proposal_id: str, fields: dict) -> str:
        resp = self._post("/api/v1/experiment/sessions/", {
            "proposal_id": proposal_id,
            "resource_id": self._resource_id,
            "objective":   f"Tomography — GUP {fields['gup_id']} ({fields['run_name']})",
        })
        return resp["session_id"]

    def _add_team_to_session(self, session_id: str, team: list[dict]) -> None:
        for member in team:
            role = "pi" if member["role"] == "PI" else "team_member"
            self._post(f"/api/v1/experiment/sessions/{session_id}/team", {
                "user_id": member["user_id"],
                "role":    role,
            })

    def _start_session(self, session_id: str) -> None:
        self._post(f"/api/v1/experiment/sessions/{session_id}/start", {})

    # ── Safety approvals ──────────────────────────────────────────────────────

    def _sync_approval(self, user_id: str, proposal_id: str, session_id: str) -> None:
        """Ask CORA to check and record the APS ESAF approval for this user."""
        self._post("/api/v1/scheduling/approvals/sync", {
            "user_id":     user_id,
            "proposal_id": proposal_id,
            "session_id":  session_id,
            "facility_id": self._facility_id,
        })

    # ── HTTP helpers ──────────────────────────────────────────────────────────

    def _get(self, path: str) -> Any:
        url = self._base + path
        resp = self._session.get(url, timeout=self._timeout)
        resp.raise_for_status()
        return resp.json()

    def _post(self, path: str, payload: dict) -> Any:
        url = self._base + path
        resp = self._session.post(url, json=payload, timeout=self._timeout)
        try:
            resp.raise_for_status()
        except requests.HTTPError as exc:
            log.error("POST %s failed %s: %s", path, resp.status_code, resp.text)
            raise RuntimeError(f"CORA API error on POST {path}: {resp.status_code}") from exc
        if resp.content:
            return resp.json()
        return {}

    def _patch(self, path: str, payload: dict) -> Any:
        url = self._base + path
        resp = self._session.patch(url, json=payload, timeout=self._timeout)
        resp.raise_for_status()
        if resp.content:
            return resp.json()
        return {}
