"""Dynamic response composer tests.

The normal patient-facing path must be model-authored from an authoritative fact catalog.
No regex, response-code text template, or conditional sentence builder is allowed to
construct the final reply.
"""
from __future__ import annotations

import asyncio

from app.api.v1.message import _run
from app.core.response_context import build_reply_context, validate_composer_output
from app.services.dialogue import AgentTurnText
from tests.test_runner_flow import SMALL_TALK_CONTRACT, stub_io, valid_payload


def test_fact_catalog_redacts_internal_ids_and_secrets():
    context = build_reply_context(
        normalized={"message_text": "عايز موعد", "patient_id": "private-patient-id"},
        clinic_context={
            "clinic_name": "عيادة النور",
            "clinic_id": "private-clinic-id",
            "api_key": "must-not-leak",
            "doctor_directory": [{"id": "private-doctor-id", "doctor_name": "د. أحمد"}],
        },
        state_data={},
        policy={"response_code": "CONVERSATION_ONLY"},
        decision={},
        normalized_agent_output={},
        repaired_result={},
        tool_events=[],
        execution_results={},
        faq_result={},
        guard={},
    )
    serialized = str(context)
    assert "عيادة النور" in serialized
    assert "د. أحمد" in serialized
    assert "private-patient-id" not in serialized
    assert "private-clinic-id" not in serialized
    assert "private-doctor-id" not in serialized
    assert "must-not-leak" not in serialized


def test_composer_contract_accepts_only_known_evidence_ids():
    context = build_reply_context(
        normalized={"message_text": "أهلا"},
        clinic_context={"clinic_name": "عيادة النور"},
        state_data={},
        policy={"response_code": "CONVERSATION_ONLY"},
        decision={},
        normalized_agent_output={},
        repaired_result={},
        tool_events=[],
        execution_results={},
        faq_result={},
        guard={},
    )
    parsed, errors = validate_composer_output({
        "reply": "أهلاً بيك في عيادة النور، تحت أمرك.",
        "evidence_ids": ["clinic.profile"],
        "missing_information": [],
        "unsupported_claims": [],
        "grounding_status": "supported",
    }, context)
    assert errors == []
    assert parsed is not None
    assert parsed["reply"].startswith("أهلاً")

    invalid, errors = validate_composer_output({
        "reply": "موعدك مؤكد غدًا.",
        "evidence_ids": ["invented.appointment"],
        "missing_information": [],
        "unsupported_claims": [],
        "grounding_status": "supported",
    }, context)
    assert invalid is None
    assert any(error.startswith("unknown_evidence_id") for error in errors)


def test_composer_rejects_self_reported_unsupported_claims():
    context = build_reply_context(
        normalized={"message_text": "في موعد؟"},
        clinic_context={"clinic_name": "عيادة النور"},
        state_data={},
        policy={"response_code": "AVAILABILITY_LOOKUP_REQUIRED"},
        decision={},
        normalized_agent_output={},
        repaired_result={},
        tool_events=[],
        execution_results={},
        faq_result={},
        guard={},
    )
    parsed, errors = validate_composer_output({
        "reply": "في موعد الساعة 10.",
        "evidence_ids": ["patient.current_message"],
        "missing_information": [],
        "unsupported_claims": ["الساعة 10 غير موجودة في facts"],
        "grounding_status": "supported",
    }, context)
    assert parsed is None
    assert "composer_reported_unsupported_claims" in errors


def test_runner_uses_composer_reply_and_passes_tool_facts(monkeypatch):
    stub_io(monkeypatch)
    import app.api.v1.message as runner

    tool_result = {
        "results": [{"title": "العنوان", "content": "شارع الملك فهد"}],
        "count": 1,
    }
    turn = AgentTurnText(
        SMALL_TALK_CONTRACT,
        tool_events=[{
            "name": "Search_Clinic_FAQ",
            "arguments": {"query": "العنوان"},
            "result": tool_result,
            "cache_hit": False,
        }],
        llm_calls=2,
    )

    async def _turn(*args, **kwargs):
        return turn

    captured = {}

    async def _composer(context):
        captured.update(context)
        tool_fact = next(f for f in context["facts"] if f["id"] == "tool.0.Search_Clinic_FAQ")
        assert tool_fact["authority"] == "database"
        assert "شارع الملك فهد" in str(tool_fact["value"])
        return {
            "reply": "عنوان العيادة في شارع الملك فهد. تحب أساعدك بحاجة تانية؟",
            "evidence_ids": ["tool.0.Search_Clinic_FAQ"],
            "missing_information": [],
            "unsupported_claims": [],
            "grounding_status": "supported",
            "raw_output": "{}",
        }

    monkeypatch.setattr(runner.dialogue, "call_primary_model_with_tool", _turn)
    monkeypatch.setattr(runner.dialogue, "compose_patient_reply", _composer)

    result = asyncio.run(_run(valid_payload(message_text="عنوان العيادة فين؟"), {}))
    assert result["reply_text"] == "عنوان العيادة في شارع الملك فهد. تحب أساعدك بحاجة تانية؟"
    assert "tool.0.Search_Clinic_FAQ" in captured["fact_ids"]
    assert result["_debug"]["deterministic_override"] is False


def test_runner_falls_back_to_model_draft_when_composer_fails(monkeypatch):
    stub_io(monkeypatch)
    import app.api.v1.message as runner

    async def _fail(_context):
        raise RuntimeError("composer unavailable")

    monkeypatch.setattr(runner.dialogue, "compose_patient_reply", _fail)
    result = asyncio.run(_run(valid_payload(message_text="أهلا"), {}))

    assert result["reply_text"] == "أهلاً بك! كيف أقدر أساعدك؟"
    assert result["response_code"] == "CONVERSATION_ONLY"


def test_audit_records_tool_trace_and_real_usage(monkeypatch):
    """Regression guard: replacing the template extractor with the composer dropped the
    tool trace from the audit input (tool_call_count silently became 0) and usage rows
    fell back to char-count estimates. Both are wired explicitly now."""
    stub_io(monkeypatch)
    import app.api.v1.message as runner
    import app.db.repository as repo

    captured: dict = {}

    async def _audit(entry):
        captured["audit"] = entry
        return {}

    usage_rows: list = []

    async def _usage(row):
        usage_rows.append(row)
        return "INSERT 0 1"

    monkeypatch.setattr(repo, "log_agent_audit_entry", _audit)
    monkeypatch.setattr(repo, "insert_ai_request_usage", _usage)

    turn = AgentTurnText(
        SMALL_TALK_CONTRACT,
        tool_events=[{
            "name": "Search_Clinic_FAQ",
            "arguments": {"query": "العنوان"},
            "result": {"results": [{"title": "t", "content": "c"}], "count": 1},
            "cache_hit": False,
        }],
        llm_calls=2,
        usage=[{"prompt_tokens": 100, "completion_tokens": 20, "total_tokens": 120}],
    )

    async def _turn(*args, **kwargs):
        return turn

    async def _composer(context):
        return {
            "reply": "تحت أمرك.",
            "evidence_ids": ["patient.current_message"],
            "missing_information": [],
            "unsupported_claims": [],
            "grounding_status": "supported",
            "raw_output": "{}",
            "usage": {"prompt_tokens": 300, "completion_tokens": 40, "total_tokens": 340},
        }

    monkeypatch.setattr(runner.dialogue, "call_primary_model_with_tool", _turn)
    monkeypatch.setattr(runner.dialogue, "compose_patient_reply", _composer)

    asyncio.run(_run(valid_payload(message_text="العيادة فين؟"), {}))

    audit = captured["audit"]
    assert audit["tool_call_count"] == 1
    assert audit["tool_calls"][0]["name"] == "Search_Clinic_FAQ"
    assert audit["reply_composer"]["origin"] == "model_composer"
    assert audit["reply_composer"]["tool_event_count"] == 1

    nodes = {r["metadata"] and __import__("json").loads(r["metadata"]).get("model_node") for r in usage_rows}
    assert "DeepSeek Model" in nodes
    assert "Result Reply Composer" in nodes
    totals = {__import__("json").loads(r["metadata"]).get("model_node"): r["total_tokens"] for r in usage_rows}
    # provider-reported totals, not the char-count estimate
    assert totals["DeepSeek Model"] == 120
    assert totals["Result Reply Composer"] == 340
