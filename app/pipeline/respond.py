"""Faithful port of the n8n `Respond To Patient` node response body.

Source: n8n_reference/extracted/respond_to_patient_body.js — the final HTTP response contract
sent to the WhatsApp sender. Keys and precedence are identical.
"""
from __future__ import annotations

from typing import Any, Dict, Optional


def _has_transport_error(value: Optional[Dict[str, Any]]) -> bool:
    """JS hasTransportError: error | errorMessage | errorDetails | statusCode>=400 | status>=400."""
    if not value:
        return False
    try:
        status_code = int(value.get("statusCode") or 0)
    except (TypeError, ValueError):
        status_code = 0
    try:
        status = int(value.get("status") or 0)
    except (TypeError, ValueError):
        status = 0
    return bool(value.get("error") or value.get("errorMessage") or value.get("errorDetails")
                or status_code >= 400 or status >= 400)


def build_final_response(
    normalized: Dict[str, Any],
    outgoing_message_id: Optional[str],
    save_initial: Dict[str, Any],
    save_retry: Optional[Dict[str, Any]],
    reply_guard_result: Optional[Dict[str, Any]],
    rendered_reply: Optional[str],
    response_policy_output: Dict[str, Any],
    processed_at_iso: str,
) -> Dict[str, Any]:
    """Source node: Respond To Patient (responseBody expression).

    reply_text precedence (post no-static-replies, 2026-09-18):
      1. rendered_reply — the model-authored reply, always preferred; it ships even
         when the state save failed (the failure travels in metadata, and when
         NOTHING was authored the turn suppresses so the retry re-runs it).
      2. _reply_guard.override — legacy field, now always None.

    The legacy save-failure notice is gone: apply_reply_guard no longer writes
    override text and no prewritten sentence ever stands in for the model.
    """
    initial = save_initial or {}
    retry = save_retry or {}
    retry_ran = len(retry) > 0
    failed = (
        _has_transport_error(initial)
        or _has_transport_error(retry)
        or (retry_ran and retry.get("saved") is not True)
        or (not retry_ran and initial.get("saved") is False
            and initial.get("rejected_reason") != "CONCURRENT_STATE_STALE")
    )
    if failed:
        # Reviewer/user directive (2026-09-18, no-static-replies): a state-save failure
        # must NOT overwrite the model-authored reply — the patient still gets their
        # answer, the save failure travels in metadata, and when NOTHING was authored
        # the turn suppresses (reply_text None) so the retry re-runs it.
        reply_text = rendered_reply or None
    else:
        guard = reply_guard_result or {}
        override = ((guard.get("_reply_guard") or {}).get("override")) if isinstance(guard.get("_reply_guard"), dict) else None
        # Model first. `override` is a legacy field that apply_reply_guard now always
        # sets to None; if it ever carries text again it must not outrank the model.
        reply_text = rendered_reply or override

    out = response_policy_output or {}
    guard = reply_guard_result or {}
    # True only when a prewritten override was actually the text sent. Since the model's
    # reply now outranks it, this is False whenever the model produced anything.
    deterministic_override = bool(not failed and not rendered_reply and override)
    return {
        "reply_text": reply_text,
        "suppress_reply": bool(failed and not reply_text),
        "conversation_id": (normalized or {}).get("conversation_id"),
        "clinic_id": (normalized or {}).get("clinic_id"),
        "outgoing_message_id": outgoing_message_id or None,
        "response_code": out.get("response_code"),
        "metadata": {
            "intent": (out.get("output") or {}).get("intent"),
            "operation_status": (out.get("output") or {}).get("operation_status"),
            "appointment_id": (out.get("output") or {}).get("appointment_id"),
            "escalate": (out.get("output") or {}).get("escalate"),
            "proposed_action": (out.get("output") or {}).get("proposed_action"),
            "processed_at": processed_at_iso,
        },
        "_debug": {"deterministic_override": deterministic_override},
    }
