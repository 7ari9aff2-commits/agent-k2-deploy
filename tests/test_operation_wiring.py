"""Wiring regression tests (2026-09-17 multi-agent review).

The runner passed mis-keyed input envelopes to Validate Child Envelope /
Prepare Operation Finalize Input / Persist Pending Confirmation, so every
successful mutation finalized as CHILD_CONTRACT_INVALID → INCONCLUSIVE and
confirmations were never persisted. These tests pin the corrected shapes.
"""
from __future__ import annotations

import asyncio
import uuid

from app.pipeline import stages_post
from app.db import repository

CLINIC = str(uuid.UUID("123e4567-e89b-42d3-a456-426614174000"))
APPOINTMENT = str(uuid.UUID("123e4567-e89b-42d3-a456-426614174003"))

DECISION = {
    "system_decision": {"action": "create_appointment",
                        "confirmation_target": {"action": "create_appointment",
                                                "appointment_id": APPOINTMENT}},
    "response_code": "APPOINTMENT_CREATED",
    "booking_context": {"doctor_name": "د. أحمد"},
    "confirmation_target": {"action": "create_appointment"},
}
NORMALIZED = {"clinic_id": CLINIC, "idempotency_key": "telegram:ch:1", "correlation_id": "corr-1"}
CLAIM_APPLIED = {"operation_id": "op-1", "child_execution_allowed": True}
EXEC_ROW = {"id": APPOINTMENT, "success": True, "response_code": "APPOINTMENT_CREATED",
            "appointment_id": APPOINTMENT, "booking_number": "BK-100", "public_id": "BK-100"}


def test_validate_child_envelope_with_corrected_runner_wiring_accepts_create():
    envelope = stages_post.validate_child_envelope(EXEC_ROW, {
        "system_orchestrator": DECISION, "normalize_validate": NORMALIZED,
        "apply_operation_claim": CLAIM_APPLIED}, execution_id="exec-1")
    assert envelope["child_contract_valid"] is True, envelope.get("contract_error")
    assert envelope["response_code"] == "CREATE_COMPLETED"
    assert envelope["operation_id"] == "op-1"


def test_validate_child_envelope_with_old_wiring_failsdocuments_the_bug():
    """Documents the pre-fix behavior: old keys → every create looked invalid."""
    envelope = stages_post.validate_child_envelope(EXEC_ROW, {
        "normalized": NORMALIZED, "decision": DECISION}, execution_id="exec-1")
    assert envelope["child_contract_valid"] is False
    assert envelope["contract_error"] == "UNEXPECTED_OPERATION"


def test_prepare_operation_finalize_input_reads_normalize_validate():
    finalize_input = stages_post.prepare_operation_finalize_input(
        {"child_contract_checked": True, "child_contract_valid": True, "success": True,
         "operation_id": "op-1", "response_code": "CREATE_COMPLETED"},
        {"normalize_validate": {"clinic_id": CLINIC}}, execution_id="exec-1")
    assert finalize_input["finalize_clinic_id"] == CLINIC
    assert finalize_input["finalize_operation_id"] == "op-1"
    assert finalize_input["finalize_status"] == "COMPLETED"
    assert finalize_input["finalize_mutation_status"] == "EXECUTED"


def test_merge_operation_completion_consumes_the_finalized_row():
    envelope = stages_post.validate_child_envelope(EXEC_ROW, {
        "system_orchestrator": DECISION, "normalize_validate": NORMALIZED,
        "apply_operation_claim": CLAIM_APPLIED}, execution_id="exec-1")
    finalized = {"operation_id": "op-1", "operation_status": "COMPLETED",
                 "mutation_status": "EXECUTED", "child_execution_id": "exec-1"}
    merged = stages_post.merge_operation_completion(envelope, finalized)
    assert merged["operation_finalized"] is True
    assert merged["operation_ledger_status"] == "COMPLETED"
    assert merged["operation_mutation_status"] == "EXECUTED"


def test_persist_pending_confirmation_ctx_shape_reaches_sql_params(monkeypatch):
    captured = {}

    async def fake_pool():
        class _Ctx:
            async def __aenter__(self):
                return self

            async def __aexit__(self, *exc):
                return False

            async def execute(self, *args):
                return "OK"

        class _Pool:
            def acquire(self):
                return _Ctx()

        return _Pool()

    async def fake_fetchrow(conn, sql, *params):
        captured["params"] = params
        return None

    monkeypatch.setattr("app.db.pool.get_pool", fake_pool)
    monkeypatch.setattr(repository, "_fetchrow", fake_fetchrow)

    ctx = {"normalized": NORMALIZED, **{
        "response_code": "CONFIRMATION_REQUIRED",
        "confirmation_target": {"confirmation_id": "cf-1", "action": "create_appointment"},
        "booking_context": {},
        "system_decision": DECISION["system_decision"],
    }}
    asyncio.run(repository.persist_pending_confirmation(ctx))
    params = captured["params"]
    assert params[0] == "CONFIRMATION_REQUIRED", "SQL gate must see the response code"

    legacy = {"normalized": NORMALIZED, "decision": DECISION}
    captured["params"] = None
    asyncio.run(repository.persist_pending_confirmation(legacy))
    assert captured["params"][0] == "", "documents the pre-fix bug: gate never fired"
