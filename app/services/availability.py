"""Faithful 1:1 port of n8n sub-workflow `k2 - get_available_slots (deterministic)` (id 6467rQRBEA5fWc0l).

Source nodes (n8n_reference/extracted/avail/):
  Get Available Slots Input -> Resolve Clinic Timezone (postgres)
  -> Prepare Search Window (code) -> Slot Input Valid? (if)
     [invalid] -> Match & Analyze Slots (code)
     [valid]   -> Call rpc_get_available_slots (httpRequest -> here: direct Postgres RPC)
  Match & Analyze -> Needs Schedule Fallback? (if)
     [true]  -> Check Doctor Weekly Schedule (postgres) -> Build Final Response
     [false] -> Build Final Response
  -> Build Presented Offer -> Persist Presented Offer (postgres) -> Return Availability Result
"""
from __future__ import annotations

import json
import re
import uuid as uuidlib
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

from app.db.pool import get_pool

RESOLVE_CLINIC_TIMEZONE_SQL = """
SELECT
  $1 AS clinic_id,
  COALESCE(r.doctor_id, NULLIF($2, '')) AS doctor_id,
  $3 AS service_id,
  NULLIF($4, '') AS requested_date,
  NULLIF($5, '') AS requested_time,
  NULLIF($6, '') AS search_range_days,
  NULLIF($7, '') AS now_iso,
  NULLIF($8, '') AS timezone,
  NULLIF($9, '') AS timezone_source,
  NULLIF($10, '') AS utc_offset,
  NULLIF($11, '') AS message_fingerprint,
  NULLIF($12, '') AS message_id,
  NULLIF($13, '') AS request_key,
  NULLIF($14, '') AS source_event_id,
  NULLIF($15, '') AS turn_id,
  NULLIF($16, '') AS turn_key,
  NULLIF(c.timezone, '') AS clinic_timezone
FROM clinics c
LEFT JOIN LATERAL (
  SELECT d.id::text AS doctor_id
  FROM doctors d
  WHERE d.clinic_id = $1::uuid
    AND d.is_active = true AND d.deleted_at IS NULL
    AND NULLIF($2, '') IS NOT NULL
    AND $2 !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$'
    AND REGEXP_REPLACE(
          REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(d.name, '[أإآٱ]', 'ا', 'g'), 'ى', 'ي', 'g'), 'ة', 'ه', 'g'),
          '^\\s*(الدكتورة|الدكتور|دكتورة|دكتور|د\\.)\\s*', '')
      ILIKE '%' || REGEXP_REPLACE(
          REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE($2, '[أإآٱ]', 'ا', 'g'), 'ى', 'ي', 'g'), 'ة', 'ه', 'g'),
          '^\\s*(الدكتورة|الدكتور|دكتورة|دكتور|د\\.)\\s*', '') || '%'
  ORDER BY d.name
  LIMIT 1
) r ON true
WHERE c.id = $1::uuid
LIMIT 1;
"""

WEEKLY_SCHEDULE_SQL = """
SELECT day_of_week, start_time, end_time
FROM doctor_schedule_rules
WHERE clinic_id = $1::uuid
  AND doctor_id = $2::uuid
  AND day_of_week = EXTRACT(DOW FROM $3::date)
  AND rule_type = 'weekly'
  AND is_available = true
  AND deleted_at IS NULL
ORDER BY start_time ASC;
"""

PERSIST_PRESENTED_OFFER_SQL = """
WITH in_row AS (
  SELECT
    CASE WHEN $1::text ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$' THEN $1::uuid ELSE NULL END AS conversation_id,
    CASE WHEN $3::text ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$' THEN $3::uuid ELSE NULL END AS clinic_id,
    NULLIF($4::text, '') AS patient_id,
    $2::jsonb AS offer
),
upd AS (
  UPDATE conversation_state cs
  SET state_data = jsonb_set(jsonb_set(cs.state_data, '{presented_offer}', in_row.offer, true), '{pending_offer}', in_row.offer, true)
  FROM in_row
  WHERE in_row.conversation_id IS NOT NULL
    AND in_row.clinic_id IS NOT NULL
    AND cs.conversation_id = in_row.conversation_id
    AND EXISTS (
      SELECT 1 FROM conversations c
      WHERE c.id = cs.conversation_id
        AND c.clinic_id = in_row.clinic_id
        AND (in_row.patient_id IS NULL OR c.patient_id::text = in_row.patient_id)
        AND c.deleted_at IS NULL
    )
  RETURNING cs.conversation_id
)
SELECT (SELECT count(*) FROM upd)::int AS offer_persisted, in_row.offer AS presented_offer_write FROM in_row;
"""

# n8n called this via Supabase REST (named args). Direct Postgres call uses positional
# order from the REST body: p_clinic_id, p_start_date, p_end_date, p_doctor_id, p_service_id.
# PORT-TODO(n8n): verify the live function signature/order against the production DB.
RPC_GET_AVAILABLE_SLOTS_SQL = """
SELECT * FROM public.rpc_get_available_slots($1::uuid, $2::date, $3::date, $4::uuid, $5::uuid);
"""

_UUID_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$", re.I)
_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _uuid(value: Any) -> Optional[uuidlib.UUID]:
    s = str(value or "")
    return uuidlib.UUID(s) if _UUID_RE.match(s) else None


def js_iso_from_ms(ms: float) -> str:
    return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def _local_wall_clock(iso: str, tz: Optional[str]) -> Dict[str, str]:
    """JS localWallClock: en-CA Intl formatting (ISO date) with UTC-slice fallback."""
    s = str(iso or "")
    date, time = s[:10], s[11:16]
    if tz and s:
        try:
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
            local = dt.astimezone(ZoneInfo(tz))
            date = local.strftime("%Y-%m-%d")
            time = local.strftime("%H:%M")
        except Exception:
            pass
    return {"date": date, "time": time}


# ── Resolve Clinic Timezone (postgres) ──────────────────────────────────────────
async def resolve_clinic_timezone(item: Dict[str, Any]) -> Dict[str, Any]:
    """Source node: Resolve Clinic Timezone (n8n_reference/extracted/avail/Resolve_Clinic_Timezone.json)."""
    params_text = [
        str(item.get(k) or "")
        for k in ("clinic_id", "doctor_id", "service_id", "requested_date", "requested_time",
                  "search_range_days", "now_iso", "timezone", "timezone_source", "utc_offset",
                  "message_fingerprint", "message_id", "request_key", "source_event_id",
                  "turn_id", "turn_key")
    ]
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(RESOLVE_CLINIC_TIMEZONE_SQL, _uuid(params_text[0]), *params_text[1:])
    if not row:
        return dict(item)
    out = {}
    for k, v in dict(row).items():
        out[k] = str(v) if v is not None else None
    return out


# ── Prepare Search Window (code) ───────────────────────────────────────────────
def prepare_search_window(trig: Dict[str, Any]) -> Dict[str, Any]:
    """Source node: Prepare Search Window (n8n_reference/extracted/avail/Prepare_Search_Window.js)."""
    requested_date = str(trig.get("requested_date") or "").strip()
    requested_time_raw = str(trig.get("requested_time") or "").strip()
    search_mode = str(trig.get("search_mode") or "").strip().lower()
    nearby_search = search_mode in ("nearby_alternatives", "nearby", "date_alternative")
    try:
        range_days_raw = int(str(trig.get("search_range_days", "")), 10)
    except (TypeError, ValueError):
        range_days_raw = None
    requested_range_days = range_days_raw if (range_days_raw is not None and range_days_raw > 0) else 3
    # Keep the window bounded. Nearby searches may look farther than a normal exact-day lookup,
    # but never beyond 14 local calendar days in one RPC call.
    range_days = min(14, max(7, requested_range_days) if nearby_search else requested_range_days)

    def invalid(error: str, **extra: Any) -> Dict[str, Any]:
        out = dict(trig)
        out.update({"input_error": error, "start_date": None, "end_date": None, "requested_datetime": None})
        out.update(extra)
        return out

    if not (_UUID_RE.match(str(trig.get("clinic_id") or "")) and _UUID_RE.match(str(trig.get("doctor_id") or ""))):
        return invalid("INVALID_OR_MISSING_IDENTIFIER")

    clinic_timezone = str(trig.get("clinic_timezone") or "").strip()
    requested_timezone = clinic_timezone or str(trig.get("timezone") or "").strip()
    timezone_name = requested_timezone
    timezone_valid = False
    if requested_timezone:
        try:
            ZoneInfo(requested_timezone)
            timezone_valid = True
        except Exception:
            timezone_valid = False
    if not timezone_valid:
        return invalid("CLINIC_TIMEZONE_NOT_CONFIGURED", timezone=None,
                       timezone_source="clinic_timezone_invalid_or_missing")

    def add_days(date_string: str, days: int) -> str:
        y, m, d = (int(x) for x in date_string.split("-"))
        return (datetime(y, m, d) + timedelta(days=days)).strftime("%Y-%m-%d")

    def offset_for_local_wall_clock(tz: str, date_string: str, time_string: str) -> str:
        y, mo, d = (int(x) for x in date_string.split("-"))
        h, mi, se = (int(x) for x in time_string.split(":"))
        naive = datetime(y, mo, d, h, mi, se)
        off = naive.replace(tzinfo=ZoneInfo(tz)).utcoffset() or timedelta(0)
        total = int(off.total_seconds() // 60)
        sign = "+" if total >= 0 else "-"
        absolute = abs(total)
        return f"{sign}{absolute // 60:02d}:{absolute % 60:02d}"

    if not _DATE_RE.match(requested_date):
        return invalid("INVALID_OR_MISSING_DATE", timezone=timezone_name, timezone_source="clinic_configuration")

    start_date = requested_date
    end_date = add_days(requested_date, range_days)
    requested_datetime: Optional[str] = None
    if requested_time_raw:
        t = requested_time_raw
        if re.match(r"^\d{2}:\d{2}$", t):
            t += ":00"
        if re.match(r"^\d{2}:\d{2}:\d{2}$", t):
            offset = "+00:00"
            try:
                offset = offset_for_local_wall_clock(timezone_name, requested_date, t)
            except Exception:
                pass
            requested_datetime = f"{requested_date}T{t}{offset}"

    return {**trig, "search_mode": search_mode or "requested_window", "nearby_search": nearby_search,
            "timezone": timezone_name, "timezone_source": "clinic_configuration", "input_error": None,
            "start_date": start_date, "end_date": end_date, "requested_datetime": requested_datetime}


# ── Call rpc_get_available_slots (httpRequest -> direct Postgres RPC) ──────────
async def call_rpc_get_available_slots(prep: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Source node: Call rpc_get_available_slots (Deterministic). Returns slot rows or raises."""
    cid_uuid = _uuid(prep.get("clinic_id"))
    start_date = prep.get("start_date")
    end_date = prep.get("end_date")
    # Guard: the SQL casts $1::uuid/$2::date/$3::date — a bad clinic UUID or a
    # missing/empty date raises a PG cast error. Fail soft like the weekly-schedule path.
    if not cid_uuid or not start_date or not end_date:
        return []
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            RPC_GET_AVAILABLE_SLOTS_SQL,
            cid_uuid,
            start_date,
            end_date,
            _uuid(prep.get("doctor_id")),
            _uuid(prep.get("service_id")),
        )
    return [dict(r) for r in rows]


# ── Match & Analyze Slots (code) ───────────────────────────────────────────────
def match_and_analyze_slots(prep: Dict[str, Any], raw_items: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Source node: Match & Analyze Slots (Deterministic) (n8n_reference/extracted/avail/Match_Analyze_Slots_Deterministic.js)."""
    error_item = next((it for it in raw_items if it and it.get("error")), None)
    common = {
        "requested_date": prep.get("requested_date") or None,
        "requested_datetime": prep.get("requested_datetime") or None,
        "clinic_id": prep.get("clinic_id"),
        "doctor_id": prep.get("doctor_id"),
        "service_id": prep.get("service_id"),
        "search_mode": prep.get("search_mode") or "requested_window",
        "nearby_search": prep.get("nearby_search") is True,
        "search_window_start": prep.get("start_date") or None,
        "search_window_end": prep.get("end_date") or None,
        "timezone": prep.get("timezone") or None,
    }
    if prep.get("input_error"):
        return {"success": False, "matched": None, "exact_slot": None, "nearest_slots": [],
                "available_slots_today": [], "all_slots_count": 0, "needs_schedule_fallback": False,
                "error_code": prep.get("input_error"), "message": "تاريخ الحجز غير واضح، محتاجين توضيح من المريض",
                "requested_datetime": None, **common}
    if error_item:
        return {"success": False, "matched": None, "exact_slot": None, "nearest_slots": [],
                "available_slots_today": [], "all_slots_count": 0, "needs_schedule_fallback": False,
                "error_code": "RPC_ERROR", "message": "تعذر جلب الأوقات المتاحة حاليا", **common}

    slots = [s for s in raw_items if s and s.get("slot_id")]
    requested_date = prep.get("requested_date")
    requested_datetime = prep.get("requested_datetime")
    requested_time = str(prep.get("requested_time") or "").strip()[:5]
    display_timezone = str(prep.get("timezone") or "UTC").strip() or "UTC"

    def slot_local(s: Dict[str, Any]) -> Dict[str, str]:
        return _local_wall_clock(s.get("start_time"), display_timezone)

    slots_for_requested_day = [s for s in slots if slot_local(s)["date"] == requested_date]

    def slot_diff_minutes(s: Dict[str, Any], ref_ms: float) -> int:
        start = datetime.fromisoformat(str(s.get("start_time")).replace("Z", "+00:00"))
        return round(abs(start.timestamp() * 1000 - ref_ms) / 60000)

    def to_public_slot(s: Dict[str, Any], ref_ms: Optional[float] = None) -> Dict[str, Any]:
        out = {
            "slot_id": s.get("slot_id"),
            "slot_status": "available",
            "clinic_id": s.get("clinic_id") or prep.get("clinic_id"),
            "doctor_id": s.get("doctor_id") or prep.get("doctor_id"),
            "service_id": s.get("service_id") or prep.get("service_id"),
            "start_time": s.get("start_time"),
            "end_time": s.get("end_time"),
        }
        if ref_ms is not None:
            out["diff_minutes"] = slot_diff_minutes(s, ref_ms)
        return out

    def requested_wall_clock_matches(s: Dict[str, Any]) -> bool:
        local = slot_local(s)
        return bool(requested_date and requested_time and local["date"] == requested_date and local["time"] == requested_time)

    matched = False
    exact_slot: Optional[Dict[str, Any]] = None
    nearest_slots: List[Dict[str, Any]] = []
    error_code: Optional[str] = None
    message: Optional[str] = None

    if requested_datetime:
        req_ms = datetime.fromisoformat(requested_datetime.replace("Z", "+00:00")).timestamp() * 1000
        # Minute-granularity match first: the patient confirms HH:MM while slots may start
        # at HH:MM:SS — a sub-minute offset must not fail the exact match.
        exact = next((s for s in slots if requested_wall_clock_matches(s)), None)
        if exact is None:
            for s in slots:
                try:
                    start_ms = datetime.fromisoformat(str(s.get("start_time")).replace("Z", "+00:00")).timestamp() * 1000
                    end_ms = datetime.fromisoformat(str(s.get("end_time")).replace("Z", "+00:00")).timestamp() * 1000
                except Exception:
                    continue
                if req_ms >= start_ms - 60000 and req_ms < end_ms:
                    exact = s
                    break
        if exact:
            matched = True
            exact_slot = to_public_slot(exact)
            message = "الوقت المطلوب متاح"
        else:
            nearest_slots = sorted(
                (to_public_slot(s, req_ms) for s in slots if not requested_wall_clock_matches(s)),
                key=lambda x: x["diff_minutes"])[:6]
            error_code = "REQUESTED_TIME_NOT_AVAILABLE"
            message = "الوقت المطلوب غير متاح، وهذه أقرب المواعيد المتاحة" if nearest_slots else "الوقت المطلوب غير متاح حاليا"
    else:
        matched = len(slots_for_requested_day) > 0
        if matched:
            message = "تم عرض الأوقات المتاحة لهذا اليوم"
        else:
            anchor = datetime.strptime(f"{requested_date}T12:00:00", "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)
            anchor_ms = anchor.timestamp() * 1000
            nearest_slots = sorted(
                (to_public_slot(s, anchor_ms) for s in slots if slot_local(s)["date"] != requested_date),
                key=lambda x: x["diff_minutes"])[:6]
            error_code = "NO_AVAILABLE_SLOTS"
            message = "لا توجد مواعيد في اليوم المطلوب، وهذه أقرب المواعيد المتاحة" if nearest_slots else "لا توجد مواعيد متاحة في نافذة البحث"

    available_slots_today = [to_public_slot(s) for s in slots_for_requested_day
                             if not requested_datetime or not requested_wall_clock_matches(s)][:10]
    needs_schedule_fallback = len(slots) == 0
    return {"success": True, "matched": matched, "exact_slot": exact_slot, "nearest_slots": nearest_slots,
            "available_slots_today": available_slots_today, "all_slots_count": len(slots),
            "needs_schedule_fallback": needs_schedule_fallback, "error_code": error_code, "message": message,
            **common,
            "authority": "supabase.rpc_get_available_slots",
            "verification_status": ("authority_error" if error_code == "RPC_ERROR"
                                    else ("verified_available" if (matched is True or available_slots_today) else "verified_unavailable"))}


# ── Check Doctor Weekly Schedule (postgres fallback) ───────────────────────────
async def check_doctor_weekly_schedule(clinic_id: str, doctor_id: str, requested_date: str) -> List[Dict[str, Any]]:
    """Source node: Check Doctor Weekly Schedule (Fallback)."""
    cid_uuid = _uuid(clinic_id)
    doc_uuid = _uuid(doctor_id)
    if not cid_uuid or not doc_uuid or not requested_date:
        return []
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(WEEKLY_SCHEDULE_SQL, cid_uuid, doc_uuid, requested_date)
    out = []
    for r in rows:
        d = dict(r)
        for k, v in list(d.items()):
            if not isinstance(v, str):
                d[k] = str(v) if v is not None else None
        out.append(d)
    return out


# ── Build Final Response (code) ────────────────────────────────────────────────
def build_final_response(match: Dict[str, Any], schedule_rows: Optional[List[Dict[str, Any]]]) -> Dict[str, Any]:
    """Source node: Build Final Response (Deterministic) (n8n_reference/extracted/avail/Build_Final_Response_Deterministic.js)."""
    fallback_expected = match.get("needs_schedule_fallback") is True
    nearby_search = match.get("nearby_search") is True or str(match.get("search_mode") or "").lower() == "nearby_alternatives"
    doctor_works_that_day = None
    working_hours = None
    error_code = match.get("error_code")
    message = match.get("message")
    verification_status = match.get("verification_status") or ("verified_unavailable" if match.get("success") is True else "authority_error")
    authority = match.get("authority") or "supabase.rpc_get_available_slots"
    if fallback_expected and schedule_rows is not None:
        valid_rows = [r for r in schedule_rows if r and r.get("day_of_week") is not None
                      and r.get("start_time") is not None and r.get("end_time") is not None]
        if nearby_search:
            # An empty RPC result over a nearby window does not prove that the doctor is off
            # on the anchor day and must not collapse the entire search into that wording.
            doctor_works_that_day = len(valid_rows) > 0
            working_hours = [{"start_time": r.get("start_time"), "end_time": r.get("end_time")} for r in valid_rows]
            error_code = "NO_AVAILABLE_SLOTS_IN_WINDOW"
            message = "لا توجد مواعيد متاحة مؤكدة في الأيام القريبة التي تم البحث فيها"
        elif len(valid_rows) == 0:
            doctor_works_that_day = False
            working_hours = []
            error_code = "DOCTOR_NOT_WORKING_THAT_DAY"
            message = "الدكتور ما يشتغلش في اليوم ده جرب يوم تاني"
        else:
            doctor_works_that_day = True
            working_hours = [{"start_time": r.get("start_time"), "end_time": r.get("end_time")} for r in valid_rows]
            error_code = "NO_AVAILABLE_SLOTS"
            message = "الدكتور يعمل في هذا اليوم، لكن لا توجد مواعيد متاحة مؤكدة حاليًا"
    exact_slot = match.get("exact_slot") or None
    return {
        "success": match.get("success"),
        "matched": match.get("matched"),
        "exact_slot": exact_slot,
        "nearest_slots": match.get("nearest_slots"),
        "available_slots_today": match.get("available_slots_today"),
        "doctor_works_that_day": doctor_works_that_day,
        "working_hours": working_hours,
        "error_code": error_code,
        "message": message,
        "requested_date": match.get("requested_date"),
        "requested_datetime": match.get("requested_datetime"),
        "clinic_id": match.get("clinic_id") or None,
        "doctor_id": match.get("doctor_id") or None,
        "service_id": match.get("service_id") or None,
        "search_mode": match.get("search_mode") or "requested_window",
        "nearby_search": nearby_search,
        "search_window_start": match.get("search_window_start") or None,
        "search_window_end": match.get("search_window_end") or None,
        "authority": authority,
        "verification_status": verification_status,
        "timezone": match.get("timezone") or None,
        "slot_status": (exact_slot or {}).get("slot_status") if exact_slot else None,
    }


# ── Build Presented Offer (code, P-OFFER) ──────────────────────────────────────
def build_presented_offer(resp: Dict[str, Any], trigger: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Source node: Build Presented Offer (Deterministic) (P-OFFER 2026-09-05)."""
    tz = str(resp.get("timezone") or "").strip() or None
    exact = resp.get("exact_slot") if isinstance(resp.get("exact_slot"), dict) else None
    today_slots = resp.get("available_slots_today") if isinstance(resp.get("available_slots_today"), list) else []
    near_slots = resp.get("nearest_slots") if isinstance(resp.get("nearest_slots"), list) else []
    raw: List[Dict[str, Any]] = []
    if exact and exact.get("slot_id"):
        raw.append(exact)
    for s in list(today_slots) + list(near_slots):
        if len(raw) >= 4:
            break
        if s and s.get("slot_id") and not any(x.get("slot_id") == s.get("slot_id") for x in raw):
            raw.append(s)
    offer = None
    if resp.get("verification_status") == "verified_available" and raw:
        now_ms = datetime.now(timezone.utc).timestamp() * 1000
        offer = {
            "schema_version": 2,
            "kind": "presented_offer",
            "offered_at": js_iso_from_ms(now_ms),
            "expires_at": js_iso_from_ms(now_ms + 600000),
            "clinic_id": resp.get("clinic_id") or None,
            "patient_id": trigger.get("patient_id") or None,
            "conversation_id": trigger.get("conversation_id") or None,
            "doctor_id": resp.get("doctor_id") or None,
            "doctor_name": None,
            "service_id": resp.get("service_id") or None,
            "appointment_type": None,
            "alternatives": [],
        }
        for i, s in enumerate(raw[:4]):
            local = _local_wall_clock(s.get("start_time"), tz)
            offer["alternatives"].append({
                "rank": i + 1,
                "slot_id": s.get("slot_id") or None,
                "start_time": s.get("start_time") or None,
                "local_date": local["date"],
                "local_time": local["time"],
                "label": None,
                "doctor_id": s.get("doctor_id") or resp.get("doctor_id") or None,
                "service_id": s.get("service_id") or resp.get("service_id") or None,
                "clinic_id": s.get("clinic_id") or resp.get("clinic_id") or None,
                "slot_status": "available",
            })
    return offer


# ── Persist Presented Offer (postgres) ─────────────────────────────────────────
async def persist_presented_offer(item: Dict[str, Any], trigger: Dict[str, Any]) -> Dict[str, Any]:
    """Source node: Persist Presented Offer (Deterministic). Writes even a JSON null offer, as n8n did."""
    offer_json = json.dumps(item.get("presented_offer_write"), ensure_ascii=False, separators=(",", ":"))
    params = [
        str(trigger.get("conversation_id") or ""),
        offer_json,
        str(item.get("clinic_id") or ""),
        str(trigger.get("patient_id") or ""),
    ]
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(PERSIST_PRESENTED_OFFER_SQL, params[0], params[1], params[2], params[3])
    if not row:
        return {"offer_persisted": 0, "presented_offer_write": item.get("presented_offer_write")}
    out = dict(row)
    if isinstance(out.get("presented_offer_write"), str):
        out["presented_offer_write"] = json.loads(out["presented_offer_write"])
    return out


# ── Entry point (mirrors the sub-workflow execution order) ─────────────────────
async def check_available_slots(trigger_input: Dict[str, Any]) -> Dict[str, Any]:
    """Runs the availability sub-workflow end to end and returns Build Final Response output."""
    item = dict(trigger_input or {})
    resolved = await resolve_clinic_timezone(item)
    prep = prepare_search_window(resolved)
    # Slot Input Valid? — true branch (input_error notEmpty) skips the RPC call.
    if prep.get("input_error"):
        match = match_and_analyze_slots(prep, [prep])
    else:
        try:
            rows = await call_rpc_get_available_slots(prep)
            match = match_and_analyze_slots(prep, rows)
        except Exception as exc:  # n8n HTTP failure surfaces as an error item into Match & Analyze
            match = match_and_analyze_slots(prep, [{"error": str(exc)}])
    schedule_rows: Optional[List[Dict[str, Any]]] = None
    if match.get("needs_schedule_fallback") is True:
        schedule_rows = await check_doctor_weekly_schedule(
            str(prep.get("clinic_id") or ""), str(prep.get("doctor_id") or ""), str(prep.get("requested_date") or ""))
    final = build_final_response(match, schedule_rows)
    offer = build_presented_offer(final, item)
    final = {**final, "presented_offer_write": offer}
    await persist_presented_offer(final, item)
    # Return Availability Result (Deterministic): pass-through, persistence internals hidden.
    return final
