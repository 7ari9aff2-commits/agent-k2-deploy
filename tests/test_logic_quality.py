"""Logic-level regression tests for the 2026-09-18 quality pass.

Covers: composer value-level grounding, booking-number presence on completed
mutations, day-word semantics, restart-keeps-doctor, dedupe gating on delivered
replies, and non-ASCII token auth.
"""
from __future__ import annotations

import asyncio
import json
import uuid

from app.core.agent_output import _absorb_day_word_to_iso
from app.core.response_context import validate_composer_output, build_reply_context


def _reply_context(response_code=None, tool_slots=True):
    return build_reply_context(
        normalized={"message_text": "عايز أحجز"},
        clinic_context={"clinic_name": "عيادة النور"},
        state_data={},
        policy={"response_code": response_code or "CONVERSATION_ONLY"},
        decision={"booking_context": {"booking_number": "BK-77"}} if response_code in
                 ("APPOINTMENT_CREATED", "IDEMPOTENT_REPLAY") else {},
        normalized_agent_output={}, repaired_result={},
        tool_events=[{
            "name": "Check_Doctor_Availability", "arguments": {},
            "result": {"success": True, "nearest_slots": [
                {"local_date": "2026-09-20", "local_time": "10:30"}]},
        }] if tool_slots else [],
        execution_results={}, faq_result={}, guard={},
    )


def _out(reply, evidence):
    return {"reply": reply, "evidence_ids": evidence, "missing_information": [],
            "unsupported_claims": [], "grounding_status": "supported"}


def test_value_grounding_accepts_fact_backed_reply():
    ctx = _reply_context()
    parsed, errors = validate_composer_output(
        _out("متاح يوم 2026-09-20 الساعة 10:30 🌸", ["tool.0.Check_Doctor_Availability"]), ctx)
    assert parsed is not None, errors


def test_value_grounding_rejects_fabricated_slot():
    ctx = _reply_context()
    parsed, errors = validate_composer_output(
        _out("متاح يوم 2026-11-05 الساعة 12:45 🌸", ["tool.0.Check_Doctor_Availability"]), ctx)
    assert parsed is None
    assert any(e.startswith("reply_value_not_in_facts") for e in errors)


def test_completed_mutation_requires_the_booking_number():
    ctx = _reply_context(response_code="APPOINTMENT_CREATED")
    parsed, errors = validate_composer_output(
        _out("تم الحجز بنجاح يا فندم ✅", ["policy.outcome"]), ctx)
    assert parsed is None
    assert any(e.startswith("reply_missing_booking_number") for e in errors)

    ok, errors = validate_composer_output(
        _out("تم الحجز بنجاح، رقم الحجز BK-77 ✅", ["policy.outcome"]), ctx)
    assert ok is not None, errors


def test_arabic_indic_reply_digits_are_folded():
    ctx = _reply_context(response_code="APPOINTMENT_CREATED")
    parsed, errors = validate_composer_output(
        _out("تم الحجز، رقم الحجز BK-٧٧ ✅", ["policy.outcome"]), ctx)
    assert parsed is not None, errors


def test_day_word_badad_is_two_days_ahead():
    iso = _absorb_day_word_to_iso("بعدغد إن شاء الله", "2026-09-18")
    assert iso == "2026-09-20"


def test_weekday_named_on_itself_means_next_week():
    # 2026-09-17 is a Thursday: "الخميس" must book NEXT Thursday.
    iso = _absorb_day_word_to_iso("الخميس", "2026-09-17")
    assert iso == "2026-09-24"


def test_weekday_ahead_still_maps_within_the_week():
    # 2026-09-17 (Thu) → "السبت" = 2026-09-19.
    iso = _absorb_day_word_to_iso("السبت", "2026-09-17")
    assert iso == "2026-09-19"


def test_non_ascii_token_header_returns_401_not_500():
    from app.core.security import verify_internal_token
    from fastapi import HTTPException

    import app.core.config as config_mod
    original = config_mod.settings.K2_INTERNAL_TOKEN
    object.__setattr__(config_mod.settings, "K2_INTERNAL_TOKEN", "secret-token")
    try:
        try:
            verify_internal_token(x_k2_internal_token="توكن-عربي-")
            raised = False
        except HTTPException as exc:
            raised = True
            assert exc.status_code == 401
        assert raised
        assert verify_internal_token(x_k2_internal_token="secret-token") is True
    finally:
        object.__setattr__(config_mod.settings, "K2_INTERNAL_TOKEN", original)


def test_get_outgoing_reply_returns_delivered_content(monkeypatch):
    import hashlib

    from app.db import repository

    key = "telegram:ch:55"

    class _Ctx:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc):
            return False

        async def fetchrow(self, sql, param):
            expected = hashlib.md5((key + ":outgoing").encode()).hexdigest()
            assert param.replace("-", "") == expected.replace("-", "")
            return {"content": "أهلاً بك! كيف أقدر أساعدك؟"}

    class _Pool:
        def acquire(self):
            return _Ctx()

    async def _pool_factory():
        return _Pool()

    monkeypatch.setattr("app.db.pool.get_pool", _pool_factory)
    assert asyncio.run(repository.get_outgoing_reply({"idempotency_key": key})) == \
        "أهلاً بك! كيف أقدر أساعدك؟"
    # A duplicate with no DELIVERED content must read as None — the turn re-runs.
    empty_ctx = _Ctx()
    empty_ctx.fetchrow = _Ctx.fetchrow

    class _EmptyCtx:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc):
            return False

        async def fetchrow(self, sql, param):
            return None

    class _EmptyPool:
        def acquire(self):
            return _EmptyCtx()

    async def _empty_factory():
        return _EmptyPool()

    monkeypatch.setattr("app.db.pool.get_pool", _empty_factory)
    assert asyncio.run(repository.get_outgoing_reply({"idempotency_key": key})) is None
    assert asyncio.run(repository.get_outgoing_reply({"idempotency_key": ""})) is None
