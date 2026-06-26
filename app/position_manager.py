"""Position-manager API surface.

This module intentionally does not contain entry-strategy logic.  It only wraps
existing reconciliation/lifecycle helpers from ``app.main`` behind a small router
so position management can evolve independently from signal generation.
"""

import os
from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional

from fastapi import APIRouter, Request


JsonDict = Dict[str, Any]


@dataclass(frozen=True)
class PositionManagerDeps:
    """Callbacks supplied by app.main to avoid importing main from this module."""

    auth_ok: Callable[[Request], bool]
    normalize_symbol: Callable[[Any], str]
    utc_now_iso: Callable[[], str]
    binance_env: Callable[[], str]
    execution_mode: Callable[[], str]
    safety_summary: Callable[[str, str], JsonDict]
    reconcile_state: Callable[[str, str, str], JsonDict]
    append_event: Callable[[JsonDict], None]


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in ("1", "true", "yes", "y", "on")


def _manager_enabled() -> bool:
    return _env_bool("POSITION_MANAGER_ENABLED", True)


def _payload_symbol(payload: JsonDict, deps: PositionManagerDeps) -> str:
    return deps.normalize_symbol(payload.get("symbol") or payload.get("pair") or "")


def _classify_reconcile(recon: JsonDict) -> str:
    state = str(recon.get("mismatch_state") or "UNKNOWN").upper()
    if state == "CLEAN":
        return "CLEAN"
    if state in ("POSITION_OPEN_PROTECTED",):
        return "MONITOR"
    if state in ("STALE_ALGO_NO_POSITION", "UNPROTECTED_POSITION", "STATE_OPEN_NO_POSITION"):
        return "ACTION_REQUIRED"
    return "UNKNOWN"


def create_router(deps: PositionManagerDeps) -> APIRouter:
    router = APIRouter(prefix="/position-manager", tags=["position-manager"])

    @router.get("/status")
    def status(request: Request) -> JsonDict:
        if not deps.auth_ok(request):
            return {"ok": False, "decision": "REJECT", "reason": "unauthorized"}
        return {
            "ok": True,
            "enabled": _manager_enabled(),
            "execution_mode": deps.execution_mode(),
            "binance_env": deps.binance_env(),
            "routes": [
                "GET /position-manager/status",
                "POST /position-manager/reconcile",
                "POST /position-manager/tick",
            ],
            "timestamp_utc": deps.utc_now_iso(),
        }

    @router.post("/reconcile")
    async def reconcile(request: Request) -> JsonDict:
        if not deps.auth_ok(request):
            return {"ok": False, "decision": "REJECT", "reason": "unauthorized"}
        try:
            payload = await request.json()
        except Exception:
            payload = {}
        if not isinstance(payload, dict):
            payload = {}

        symbol = _payload_symbol(payload, deps)
        signal_key = str(payload.get("signal_key") or payload.get("signal_id") or "").strip()
        ignore_signal_key = str(payload.get("ignore_signal_key") or "").strip()
        if not symbol:
            return {"ok": False, "decision": "REJECT", "reason": "missing_symbol"}

        recon = deps.reconcile_state(symbol, signal_key, ignore_signal_key)
        decision = _classify_reconcile(recon)
        event = {
            "event_at_utc": deps.utc_now_iso(),
            "action": "POSITION_MANAGER_RECONCILE",
            "symbol": symbol,
            "signal_key": signal_key or None,
            "decision": decision,
            "mismatch_state": recon.get("mismatch_state"),
            "cleanup_required": recon.get("cleanup_required"),
            "reasons": recon.get("reasons") or [],
        }
        deps.append_event(event)
        return {"ok": bool(recon.get("ok")), "decision": decision, "symbol": symbol, "signal_key": signal_key or None, "reconcile": recon}

    @router.post("/tick")
    async def tick(request: Request) -> JsonDict:
        if not deps.auth_ok(request):
            return {"ok": False, "decision": "REJECT", "reason": "unauthorized"}
        try:
            payload = await request.json()
        except Exception:
            payload = {}
        if not isinstance(payload, dict):
            payload = {}

        symbol = _payload_symbol(payload, deps)
        signal_key = str(payload.get("signal_key") or payload.get("signal_id") or "").strip()
        ignore_signal_key = str(payload.get("ignore_signal_key") or "").strip()
        if not symbol:
            return {"ok": False, "decision": "REJECT", "reason": "missing_symbol"}
        if not _manager_enabled():
            return {"ok": False, "decision": "DISABLED", "reason": "position_manager_disabled", "symbol": symbol}

        safety = deps.safety_summary(symbol, ignore_signal_key=ignore_signal_key)
        recon = deps.reconcile_state(symbol, signal_key, ignore_signal_key)
        decision = _classify_reconcile(recon)
        if decision == "UNKNOWN":
            action = "NEEDS_REVIEW"
        elif bool(recon.get("cleanup_required")):
            action = "OPERATOR_REVIEW_REQUIRED"
        else:
            action = "NOOP"

        event = {
            "event_at_utc": deps.utc_now_iso(),
            "action": "POSITION_MANAGER_TICK",
            "symbol": symbol,
            "signal_key": signal_key or None,
            "decision": decision,
            "action_taken": action,
            "mismatch_state": recon.get("mismatch_state"),
            "safe_to_continue": safety.get("safe_to_continue"),
            "reasons": (recon.get("reasons") or []) + (safety.get("reasons") or []),
        }
        deps.append_event(event)
        return {
            "ok": bool(recon.get("ok")),
            "decision": decision,
            "action_taken": action,
            "symbol": symbol,
            "signal_key": signal_key or None,
            "safety_summary": safety,
            "reconcile": recon,
        }

    return router
