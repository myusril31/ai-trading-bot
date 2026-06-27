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
    cancel_stale_algo_orders: Optional[Callable[[str, list], list]] = None
    tp_lifecycle_tick: Optional[Callable[[str, str], JsonDict]] = None
    live_guarded_action: Optional[Callable[[str, str, str], JsonDict]] = None
    send_report: Optional[Callable[[JsonDict], JsonDict]] = None
    list_open_positions: Optional[Callable[[], JsonDict]] = None
    find_bot_plan: Optional[Callable[[str], JsonDict]] = None
    manager_score: Optional[Callable[[str, JsonDict, JsonDict], JsonDict]] = None
    record_decision: Optional[Callable[[JsonDict], None]] = None
    action_budget_guard: Optional[Callable[[str, str], JsonDict]] = None
    action_budget_record: Optional[Callable[[str, str, JsonDict], None]] = None


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in ("1", "true", "yes", "y", "on")


def _manager_enabled() -> bool:
    return _env_bool("POSITION_MANAGER_ENABLED", True)


def _action_enabled() -> bool:
    return _env_bool("PM_ACTION_ENABLED", False)


def _pm_kill_switch() -> bool:
    return _env_bool("PM_KILL_SWITCH", False)


def _payload_symbol(payload: JsonDict, deps: PositionManagerDeps) -> str:
    return deps.normalize_symbol(payload.get("symbol") or payload.get("pair") or "")


def _payload_action(payload: JsonDict) -> str:
    return str(payload.get("action") or payload.get("manager_action") or "").strip().upper()


def _is_live_order_action(action: str) -> bool:
    return str(action or "").upper() in ("REDUCE_50", "CLOSE_FULL", "EMERGENCY_CLOSE")


def _is_tp_lifecycle_action(action: str) -> bool:
    return str(action or "").upper() == "TP_LIFECYCLE_TICK"


def _position_amt_float(recon: JsonDict, safety: JsonDict) -> float:
    raw = recon.get("binance_position_amt") or recon.get("positionAmt") or safety.get("positionAmt") or "0"
    try:
        return float(str(raw))
    except Exception:
        return 0.0


def _classify_reconcile(recon: JsonDict) -> str:
    state = str(recon.get("mismatch_state") or "UNKNOWN").upper()
    if state == "CLEAN":
        return "CLEAN"
    if state in ("POSITION_OPEN_PROTECTED",):
        return "MONITOR"
    if state in ("STALE_ALGO_NO_POSITION", "UNPROTECTED_POSITION", "STATE_OPEN_NO_POSITION"):
        return "ACTION_REQUIRED"
    return "UNKNOWN"


def _action_blocked_reason(safety: JsonDict, recon: JsonDict, explicit_action: str = "", bot_plan: Optional[JsonDict] = None, budget: Optional[JsonDict] = None) -> Optional[str]:
    if not _manager_enabled():
        return "position_manager_disabled"
    if _pm_kill_switch():
        return "pm_kill_switch_enabled"
    if not _action_enabled():
        return "pm_action_disabled"
    if not bool(recon.get("ok")):
        return "reconcile_not_ok"
    if str(recon.get("mismatch_state") or "UNKNOWN").upper() == "UNKNOWN":
        return "reconcile_unknown"
    if _is_tp_lifecycle_action(explicit_action) and not _env_bool("PM_ENABLE_TP_LIFECYCLE_TICK", False):
        return "pm_tp_lifecycle_tick_disabled"
    if _is_live_order_action(explicit_action):
        if str(recon.get("mismatch_state") or "").upper() != "POSITION_OPEN_PROTECTED":
            return "live_order_requires_position_open_protected"
        if _position_amt_float(recon, safety) == 0.0:
            return "live_order_requires_confirmed_nonzero_position"
        if str(safety.get("execution_mode") or "").upper() != "LIVE_SMALL_CAPITAL":
            return "live_guarded_action_requires_live_small_capital"
        if str(safety.get("binance_env") or "").upper() != "LIVE":
            return "live_guarded_action_requires_live_env"
        plan = bot_plan or {}
        if _env_bool("PM_REQUIRE_BOT_PLAN", True) and not plan.get("ok"):
            return "matching_bot_plan_required"
        if plan.get("ok"):
            plan_direction = str(plan.get("direction") or "").upper()
            position_amt = _position_amt_float(recon, safety)
            actual_direction = "LONG" if position_amt > 0 else "SHORT" if position_amt < 0 else ""
            if plan_direction in ("LONG", "SHORT") and actual_direction != plan_direction:
                return "position_side_mismatch"
        if budget and not budget.get("ok", True):
            return str(budget.get("reason") or "action_budget_blocked")
    return None


def _preview_manager_action(symbol: str, signal_key: str, recon: JsonDict, explicit_action: str = "") -> JsonDict:
    mismatch_state = str(recon.get("mismatch_state") or "UNKNOWN").upper()
    if _is_live_order_action(explicit_action):
        return {"action_taken": explicit_action, "actions": [explicit_action], "reason": "requested_live_guarded_action"}
    if explicit_action in ("HOLD_PLAN", "PROTECT_ONLY"):
        return {"action_taken": explicit_action, "actions": [], "reason": "manager_verdict_no_order"}
    if mismatch_state == "STALE_ALGO_NO_POSITION":
        return {"action_taken": "CANCEL_STALE_ALGO_ORDERS", "actions": ["CANCEL_STALE_ALGO_ORDERS"], "reason": "stale_algo_no_position"}
    if mismatch_state == "POSITION_OPEN_PROTECTED" and signal_key:
        return {"action_taken": "TP_LIFECYCLE_TICK", "actions": ["TP_LIFECYCLE_TICK"], "reason": "protected_position_with_signal_key"}
    if mismatch_state in ("UNPROTECTED_POSITION", "STATE_OPEN_NO_POSITION"):
        return {"action_taken": "OPERATOR_REVIEW_REQUIRED", "actions": [], "reason": mismatch_state.lower()}
    if mismatch_state == "UNKNOWN":
        return {"action_taken": "NEEDS_REVIEW", "actions": [], "reason": "unknown_reconcile_state"}
    return {"action_taken": "NOOP", "actions": [], "reason": None}


def _execute_manager_action(symbol: str, signal_key: str, recon: JsonDict, deps: PositionManagerDeps, explicit_action: str = "") -> JsonDict:
    preview = _preview_manager_action(symbol, signal_key, recon, explicit_action)
    actions = list(preview.get("actions") or [])
    cleanup_results = []
    tp_lifecycle_result = None
    live_action_result = None
    action_taken = str(preview.get("action_taken") or "NOOP")
    reason = preview.get("reason")

    if _is_live_order_action(explicit_action) and callable(deps.live_guarded_action):
        live_action_result = deps.live_guarded_action(symbol, explicit_action, signal_key)
        action_taken = explicit_action if live_action_result.get("ok") else f"{explicit_action}_FAILED"
        reason = live_action_result.get("reason")
    elif action_taken == "CANCEL_STALE_ALGO_ORDERS" and callable(deps.cancel_stale_algo_orders):
        cleanup_results = deps.cancel_stale_algo_orders(symbol, list(recon.get("open_algo_orders") or []))
        if any(not bool(r.get("ok")) for r in cleanup_results if isinstance(r, dict)):
            reason = "stale_algo_cleanup_partial_or_failed"
    elif action_taken == "TP_LIFECYCLE_TICK" and callable(deps.tp_lifecycle_tick):
        tp_lifecycle_result = deps.tp_lifecycle_tick(symbol, signal_key)
        action = str(tp_lifecycle_result.get("action_taken") or "NOOP")
        action_taken = action if action and action != "NONE" else "NOOP"
        reason = tp_lifecycle_result.get("reason")

    return {
        "action_taken": action_taken,
        "actions": actions,
        "reason": reason or None,
        "cleanup_count": len(cleanup_results),
        "cleanup_results": cleanup_results,
        "tp_lifecycle_result": tp_lifecycle_result,
        "live_action_result": live_action_result,
    }


def _build_tick_response(symbol: str, signal_key: str, safety: JsonDict, recon: JsonDict, action_result: JsonDict, blocked_reason: Optional[str], preview_only: bool) -> JsonDict:
    live_result = action_result.get("live_action_result") or {}
    tp_result = action_result.get("tp_lifecycle_result") or {}
    lifecycle_ok = bool(tp_result.get("ok")) and str(tp_result.get("action_taken") or "NONE") != "NONE"
    cleanup_ok = action_result.get("cleanup_count", 0) > 0 and not any(not bool(r.get("ok")) for r in (action_result.get("cleanup_results") or []) if isinstance(r, dict))
    live_ok = bool(live_result.get("ok"))
    return {
        "ok": bool(recon.get("ok")),
        "decision": _classify_reconcile(recon),
        "action_taken": str(action_result.get("action_taken") or "NOOP"),
        "action_executed": bool(not preview_only and not blocked_reason and (live_ok or lifecycle_ok or cleanup_ok)),
        "action_blocked_reason": blocked_reason,
        "preview_only": bool(preview_only),
        "dry_run": bool(preview_only),
        "symbol": symbol,
        "signal_key": signal_key or None,
        "safety_summary": safety,
        "reconcile": recon,
        "manager": action_result,
    }


def _sanitize_positions_result(positions_res: JsonDict) -> JsonDict:
    """Remove raw exchange payloads from run-once responses."""
    if not isinstance(positions_res, dict):
        return {"ok": False, "reason": "invalid_positions_result", "positions": []}
    clean = dict(positions_res)
    clean.pop("raw", None)
    clean["positions"] = [
        {k: v for k, v in row.items() if k != "raw"} if isinstance(row, dict) else row
        for row in (clean.get("positions") or [])
    ]
    return clean


def create_router(deps: PositionManagerDeps) -> APIRouter:
    router = APIRouter(prefix="/position-manager", tags=["position-manager"])

    async def _payload(request: Request) -> JsonDict:
        try:
            payload = await request.json()
        except Exception:
            payload = {}
        return payload if isinstance(payload, dict) else {}

    def _routes() -> list:
        return [
            "GET /position-manager/status",
            "POST /position-manager/reconcile",
            "POST /position-manager/tick",
            "POST /position-manager/run-once",
            "POST /position-manager/action-preview",
            "POST /position-manager/send-report",
        ]

    def _context(payload: JsonDict) -> JsonDict:
        symbol = _payload_symbol(payload, deps)
        signal_key = str(payload.get("signal_key") or payload.get("signal_id") or "").strip()
        ignore_signal_key = str(payload.get("ignore_signal_key") or "").strip()
        safety = deps.safety_summary(symbol, ignore_signal_key=ignore_signal_key) if symbol else {}
        recon = deps.reconcile_state(symbol, signal_key, ignore_signal_key) if symbol else {}
        return {"symbol": symbol, "signal_key": signal_key, "ignore_signal_key": ignore_signal_key, "safety": safety, "recon": recon}

    def _run(payload: JsonDict, *, preview_only: bool) -> JsonDict:
        ctx = _context(payload)
        symbol = ctx["symbol"]
        signal_key = ctx["signal_key"]
        if not symbol:
            return {"ok": False, "decision": "REJECT", "reason": "missing_symbol", "action_executed": False, "action_blocked_reason": "missing_symbol", "preview_only": True, "dry_run": True}

        safety = ctx["safety"]
        recon = ctx["recon"]
        bot_plan = deps.find_bot_plan(symbol) if callable(deps.find_bot_plan) else {}
        payload_action = _payload_action(payload)
        mismatch_state = str(recon.get("mismatch_state") or "UNKNOWN").upper()
        lifecycle_preview = _preview_manager_action(symbol, signal_key, recon, "")
        lifecycle_action = str(lifecycle_preview.get("action_taken") or "").upper()
        can_score = mismatch_state == "POSITION_OPEN_PROTECTED" and bool(bot_plan.get("ok"))
        score = deps.manager_score(symbol, recon, bot_plan) if can_score and callable(deps.manager_score) else {}
        if score and not preview_only and callable(deps.record_decision):
            deps.record_decision(score)
        if payload_action:
            explicit_action = payload_action
        elif mismatch_state in ("STALE_ALGO_NO_POSITION", "UNPROTECTED_POSITION", "STATE_OPEN_NO_POSITION", "UNKNOWN"):
            explicit_action = lifecycle_action
        elif can_score:
            explicit_action = str(score.get("verdict_action") or score.get("verdict") or "").upper()
            if explicit_action in ("", "NOOP", "HOLD_PLAN", "PROTECT_ONLY") and lifecycle_action == "TP_LIFECYCLE_TICK" and _env_bool("PM_ENABLE_TP_LIFECYCLE_TICK", False):
                explicit_action = lifecycle_action
        elif lifecycle_action == "TP_LIFECYCLE_TICK" and _env_bool("PM_ENABLE_TP_LIFECYCLE_TICK", False):
            explicit_action = lifecycle_action
        else:
            explicit_action = "NOOP"
        budget = deps.action_budget_guard(symbol, explicit_action) if _is_live_order_action(explicit_action) and callable(deps.action_budget_guard) else {}
        blocked_reason = _action_blocked_reason(safety, recon, explicit_action, bot_plan=bot_plan, budget=budget)
        effective_preview = bool(preview_only or blocked_reason)
        if effective_preview:
            action_result = _preview_manager_action(symbol, signal_key, recon, explicit_action)
        else:
            action_result = _execute_manager_action(symbol, signal_key, recon, deps, explicit_action)
            if _is_live_order_action(explicit_action) and callable(deps.action_budget_record) and (action_result.get("live_action_result") or {}).get("ok"):
                deps.action_budget_record(symbol, explicit_action, action_result)
        response = _build_tick_response(symbol, signal_key, safety, recon, action_result, blocked_reason, effective_preview)
        response["bot_plan"] = bot_plan
        response["manager_score"] = score
        response["action_budget"] = budget
        return response

    def _run_all_open_positions(payload: JsonDict) -> JsonDict:
        positions_res = deps.list_open_positions() if callable(deps.list_open_positions) else {"ok": False, "reason": "list_open_positions_callback_missing", "positions": []}
        positions_res = _sanitize_positions_result(positions_res)
        positions = positions_res.get("positions") or []
        results = []
        for pos in positions:
            if not isinstance(pos, dict):
                continue
            pos_symbol = deps.normalize_symbol(pos.get("symbol") or "")
            if not pos_symbol:
                continue
            item_payload = dict(payload)
            item_payload["symbol"] = pos_symbol
            results.append(_run(item_payload, preview_only=False))
        actions = sum(1 for row in results if row.get("action_executed"))
        response = {"ok": bool(positions_res.get("ok")), "open_positions": len(positions), "actions": actions, "positions": positions, "results": results, "list_positions": positions_res}
        if len(positions) == 0 and positions_res.get("ok"):
            response.update({"ok": True, "open_positions": 0, "actions": 0})
        return response

    @router.get("/status")
    def status(request: Request) -> JsonDict:
        if not deps.auth_ok(request):
            return {"ok": False, "decision": "REJECT", "reason": "unauthorized"}
        return {
            "ok": True,
            "version_scope": "v0.27_live_guarded_wip",
            "enabled": _manager_enabled(),
            "pm_action_enabled": _action_enabled(),
            "pm_kill_switch": _pm_kill_switch(),
            "pm_enable_tp_lifecycle_tick": _env_bool("PM_ENABLE_TP_LIFECYCLE_TICK", False),
            "execution_mode": deps.execution_mode(),
            "binance_env": deps.binance_env(),
            "routes": _routes(),
            "timestamp_utc": deps.utc_now_iso(),
        }

    @router.post("/reconcile")
    async def reconcile(request: Request) -> JsonDict:
        if not deps.auth_ok(request):
            return {"ok": False, "decision": "REJECT", "reason": "unauthorized"}
        payload = await _payload(request)
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

    @router.post("/action-preview")
    async def action_preview(request: Request) -> JsonDict:
        if not deps.auth_ok(request):
            return {"ok": False, "decision": "REJECT", "reason": "unauthorized"}
        return _run(await _payload(request), preview_only=True)

    @router.post("/tick")
    async def tick(request: Request) -> JsonDict:
        if not deps.auth_ok(request):
            return {"ok": False, "decision": "REJECT", "reason": "unauthorized"}
        response = _run(await _payload(request), preview_only=False)
        event = {
            "event_at_utc": deps.utc_now_iso(),
            "action": "POSITION_MANAGER_TICK",
            "symbol": response.get("symbol"),
            "signal_key": response.get("signal_key"),
            "decision": response.get("decision"),
            "action_taken": response.get("action_taken"),
            "action_executed": response.get("action_executed"),
            "action_blocked_reason": response.get("action_blocked_reason"),
            "preview_only": response.get("preview_only"),
            "manager_actions": (response.get("manager") or {}).get("actions") or [],
            "manager_reason": (response.get("manager") or {}).get("reason"),
            "cleanup_count": (response.get("manager") or {}).get("cleanup_count"),
            "mismatch_state": (response.get("reconcile") or {}).get("mismatch_state"),
            "safe_to_continue": (response.get("safety_summary") or {}).get("safe_to_continue"),
            "reasons": ((response.get("reconcile") or {}).get("reasons") or []) + ((response.get("safety_summary") or {}).get("reasons") or []),
        }
        deps.append_event(event)
        return response

    @router.post("/send-report")
    async def send_report(request: Request) -> JsonDict:
        if not deps.auth_ok(request):
            return {"ok": False, "decision": "REJECT", "reason": "unauthorized"}
        payload = await _payload(request)
        ctx = _context(payload)
        report = {
            "ok": bool(ctx.get("recon", {}).get("ok")) if ctx.get("symbol") else False,
            "version_scope": "v0.27_live_guarded_wip",
            "symbol": ctx.get("symbol") or None,
            "signal_key": ctx.get("signal_key") or None,
            "safety_summary": ctx.get("safety"),
            "reconcile": ctx.get("recon"),
            "timestamp_utc": deps.utc_now_iso(),
        }
        send_result = deps.send_report(report) if callable(deps.send_report) else {
            "ok": True,
            "decision": "REPORT_PREVIEW",
            "reason": "send_report_callback_not_configured",
        }
        event = {
            "event_at_utc": deps.utc_now_iso(),
            "action": "POSITION_MANAGER_SEND_REPORT",
            "symbol": report.get("symbol"),
            "signal_key": report.get("signal_key"),
            "send_result": send_result,
        }
        deps.append_event(event)
        return {"ok": bool(send_result.get("ok")), "report": report, "send_result": send_result}

    @router.post("/run-once")
    async def run_once(request: Request) -> JsonDict:
        if not deps.auth_ok(request):
            return {"ok": False, "decision": "REJECT", "reason": "unauthorized"}
        payload = await _payload(request)
        symbol = _payload_symbol(payload, deps)
        if symbol:
            response = _run(payload, preview_only=False)
            event = {
                "event_at_utc": deps.utc_now_iso(),
                "action": "POSITION_MANAGER_RUN_ONCE",
                "response": response,
            }
            deps.append_event(event)
            return response

        response = _run_all_open_positions(payload)
        event = {
            "event_at_utc": deps.utc_now_iso(),
            "action": "POSITION_MANAGER_RUN_ONCE",
            "open_positions": response.get("open_positions"),
            "actions": response.get("actions"),
        }
        deps.append_event(event)
        return response

    return router
