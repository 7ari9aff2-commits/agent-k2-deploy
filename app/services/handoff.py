"""Port of the n8n sub-workflow "Handoff Child v1" (n8n_reference/handoff_child_v1.json).

Node chain (1:1):
    Handoff Child Input (executeWorkflowTrigger)
        -> Normalize Handoff Input (code, runOnceForEachItem)
        -> Create or Reuse Handoff Request (postgres, executeQuery)
        -> Build Handoff Structured Result (code, runOnceForEachItem)
"""

import asyncio
import json
import logging
import re
from typing import Any, Dict, Optional

from pydantic import BaseModel

logger = logging.getLogger(__name__)

__all__ = [
    "HandoffChildInput",
    "create_or_reuse_handoff",
    "normalize_handoff_input",
    "build_handoff_structured_result",
]


class HandoffChildInput(BaseModel):
    """Declared inputs of node "Handoff Child Input" (executeWorkflowTrigger).

    n8n declares 11 untyped (default string) fields plus two object fields.
    Every field is optional: the n8n caller may omit any of them (the JS then
    treats missing keys as undefined)."""

    clinic_id: Optional[str] = None
    conversation_id: Optional[str] = None
    patient_id: Optional[str] = None
    channel_type: Optional[str] = None
    channel_id: Optional[str] = None
    handoff_reason: Optional[str] = None
    reason_code: Optional[str] = None
    reason_note: Optional[str] = None
    correlation_id: Optional[str] = None
    source_message_id: Optional[str] = None
    priority: Optional[str] = None
    context_snapshot: Optional[Dict[str, Any]] = None
    metadata: Optional[Dict[str, Any]] = None


# Node "Create or Reuse Handoff Request" (postgres) — query copied verbatim from
# the workflow export (do not reformat). Parameters $1..$11 are bound positionally
# in the exact order of the node's queryReplacement expression.
_HANDOFF_RPC_SQL = """-- handoff-link-v3: resolve the channel from the conversation row itself.
-- rpc_handoff_create_or_reuse enforces conversations.channel_id = p_channel_id, so the
-- conversation's own channel is the only id that can pass the scope check. The channel_id
-- passed by the parent is kept in metadata for audit only. Empty uuids degrade to NULL so
-- the RPC returns INVALID_INPUT instead of the query failing with a cast error.
SELECT public.rpc_handoff_create_or_reuse(
  NULLIF($1::text, '')::uuid,
  NULLIF($2::text, '')::uuid,
  NULLIF($3::text, '')::uuid,
  $4::text,
  (SELECT c.channel_id
     FROM public.conversations c
    WHERE c.id = NULLIF($2::text, '')::uuid
      AND c.clinic_id = NULLIF($1::text, '')::uuid
      AND c.patient_id = NULLIF($3::text, '')::uuid
      AND c.deleted_at IS NULL),
  $5::text,
  $6::text,
  $7::text,
  NULLIF($8::text, ''),
  NULLIF($9::text, '')::uuid,
  $10::jsonb,
  $11::jsonb
) AS result;"""


_DETERMINISTIC_UUID_RE = re.compile(
    "^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$",
    re.IGNORECASE,
)


def _js_clean(value: Any) -> str:
    """JS: value === undefined || value === null ? '' : String(value).trim()."""
    if value is None:  # undefined and null both arrive as None here
        return ""
    return str(value).strip()


def _js_truthy(value: Any) -> bool:
    """JS truthiness — differs from Python: {} and [] are truthy in JS."""
    if value is None or value is False:
        return False
    if isinstance(value, (int, float)) and not isinstance(value, bool) and value == 0:
        return False
    if isinstance(value, str) and value == "":
        return False
    return True


def _js_or(value: Any, fallback: Any) -> Any:
    """JS: value || fallback."""
    return value if _js_truthy(value) else fallback


def _js_imul(a: int, b: int) -> int:
    """JS Math.imul — 32-bit integer multiplication."""
    return (a * b) & 0xFFFFFFFF


def _fnv1a_hex(source: str, seed: int) -> str:
    """JS hash(seed) closure from toDeterministicUuid (8 lowercase hex chars)."""
    h = seed & 0xFFFFFFFF  # seed >>> 0
    units = source.encode("utf-16-le")  # charCodeAt iterates UTF-16 code units
    for index in range(0, len(units), 2):
        h ^= units[index] | (units[index + 1] << 8)
        h = _js_imul(h, 16777619)
    return format(h & 0xFFFFFFFF, "08x")  # (h >>> 0).toString(16).padStart(8, '0')


def _to_deterministic_uuid(value: Any) -> str:
    """Ported helper of "Normalize Handoff Input".

    Defined but never called in the source node; kept for 1:1 fidelity."""
    raw = _js_clean(value)
    if _DETERMINISTIC_UUID_RE.match(raw):
        return raw.lower()
    source = raw or "handoff-channel"
    hex_text = "".join(
        _fnv1a_hex(source, seed)
        for seed in (0x811C9DC5, 0x9E3779B9, 0x85EBCA6B, 0xC2B2AE35)
    )
    hex_text = hex_text[:12] + "5" + hex_text[13:16] + "a" + hex_text[17:]
    return "{0}-{1}-{2}-{3}-{4}".format(
        hex_text[:8], hex_text[8:12], hex_text[12:16], hex_text[16:20], hex_text[20:32]
    )


def normalize_handoff_input(values: Dict[str, Any]) -> Dict[str, Any]:
    """Source node: Normalize Handoff Input (n8n_reference/handoff_child_v1.json)."""

    def object_or_empty(value: Any) -> Dict[str, Any]:
        # JS: value && typeof value === 'object' && !Array.isArray(value) ? value : {}
        return value if isinstance(value, dict) else {}

    raw_channel_id = _js_clean(values.get("channel_id"))
    normalized_channel_id = raw_channel_id or None  # JS: rawChannelId || null
    metadata = object_or_empty(values.get("metadata"))
    metadata = {
        **metadata,
        "raw_channel_id": raw_channel_id or None,
        "normalized_channel_id": normalized_channel_id,
    }
    return {
        "clinic_id": _js_clean(values.get("clinic_id")),
        "conversation_id": _js_clean(values.get("conversation_id")),
        "patient_id": _js_clean(values.get("patient_id")),
        "channel_type": _js_clean(values.get("channel_type")).lower(),
        "channel_id": raw_channel_id or None,
        "handoff_reason": _js_clean(values.get("handoff_reason")),
        "reason_code": _js_clean(values.get("reason_code")).upper(),
        "reason_note": _js_clean(values.get("reason_note")) or None,
        "correlation_id": _js_clean(values.get("correlation_id")),
        "source_message_id": _js_clean(values.get("source_message_id")) or None,
        "priority": _js_clean(values.get("priority") or "NORMAL").upper(),
        "context_snapshot": object_or_empty(values.get("context_snapshot")),
        "metadata": metadata,
    }


def build_handoff_structured_result(
    row: Optional[Dict[str, Any]], normalized: Dict[str, Any]
) -> Dict[str, Any]:
    """Source node: Build Handoff Structured Result (n8n_reference/handoff_child_v1.json)."""
    raw = row or {}  # JS: $json || {}
    result: Any = raw.get("result")
    if result is None:  # JS: raw.result ?? raw — ?? falls through on null/undefined only
        result = raw
    if isinstance(result, str):
        try:
            result = json.loads(result)
        except Exception:  # JS: catch (error) — keep the same fallback shape
            result = {"success": False, "code": "DATABASE_ERROR", "handoff": None}
    if not isinstance(result, dict):
        # JS member access on a non-object yields undefined for every field.
        result = {}
    success = result.get("success") is True  # JS: result.success === true
    return {
        "success": success,
        "code": _js_or(result.get("code"), "HANDOFF_CREATED" if success else "INTERNAL_ERROR"),
        "handoff": _js_or(result.get("handoff"), None),
        "conversation_id": _js_or(
            _js_or(result.get("conversation_id"), normalized.get("conversation_id")), None
        ),
        "correlation_id": _js_or(
            _js_or(result.get("correlation_id"), normalized.get("correlation_id")), None
        ),
        "created": result.get("created") is True,  # JS: result.created === true
        "audit_status": _js_or(result.get("audit_status"), None),
        "audit_error": _js_or(result.get("audit_error"), None),
    }


async def create_or_reuse_handoff(payload: HandoffChildInput) -> Dict[str, Any]:
    """Source workflow: Handoff Child v1 (n8n_reference/handoff_child_v1.json).

    Normalizes the declared inputs, calls public.rpc_handoff_create_or_reuse with
    the node's SQL verbatim, and returns the structured handoff envelope:
    {success, code, handoff, conversation_id, correlation_id, created,
    audit_status, audit_error}."""
    values = payload.model_dump()
    normalized = normalize_handoff_input(values)

    # Node "Create or Reuse Handoff Request" — queryReplacement order ($1..$11):
    # String(x || "") for the text params, JSON.stringify(x || {}) for the two
    # jsonb params. JSON.stringify emits compact UTF-8 JSON, hence the separators.
    parameters: list = [
        normalized["clinic_id"] or "",
        normalized["conversation_id"] or "",
        normalized["patient_id"] or "",
        normalized["channel_type"] or "",
        normalized["handoff_reason"] or "",
        normalized["reason_code"] or "",
        normalized["priority"] or "NORMAL",
        normalized["source_message_id"] or "",
        normalized["correlation_id"] or "",
        json.dumps(normalized["context_snapshot"] or {}, ensure_ascii=False, separators=(",", ":")),
        json.dumps(normalized["metadata"] or {}, ensure_ascii=False, separators=(",", ":")),
    ]

    fetched: Optional[Any] = None
    succeeded = False
    last_error: Optional[BaseException] = None
    # n8n postgres node config: retryOnFail=true, maxTries=2, waitBetweenTries=1000.
    for attempt in range(2):
        try:
            # Imported lazily so this module stays importable (and unit-testable)
            # on machines without the asyncpg driver installed.
            from app.db.pool import get_pool

            pool = await get_pool()
            async with pool.acquire() as connection:
                fetched = await connection.fetchrow(_HANDOFF_RPC_SQL, *parameters)
            succeeded = True
            break
        except Exception as error:  # mirrors the n8n node failure boundary
            last_error = error
            if attempt == 0:
                await asyncio.sleep(1.0)

    if not succeeded:
        logger.warning(f"Handoff rpc failed after retries: {last_error}")
        # PORT-TODO(n8n): the postgres node's terminal failure goes to the n8n error
        # workflow KmQZ9bXmmP1YEZht; the port returns the DATABASE_ERROR envelope
        # instead of raising, with the same conversation/correlation fallbacks the
        # Build node applies.
        return {
            "success": False,
            "code": "DATABASE_ERROR",
            "handoff": None,
            "conversation_id": _js_or(normalized["conversation_id"], None),
            "correlation_id": _js_or(normalized["correlation_id"], None),
            "created": False,
            "audit_status": None,
            "audit_error": None,
        }

    return build_handoff_structured_result(dict(fetched) if fetched else {}, normalized)
