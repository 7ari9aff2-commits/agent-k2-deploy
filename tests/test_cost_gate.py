"""Cost-gate tests: the composer LLM call is skipped when the dialogue agent's own
draft passes the same grounding contract — and still runs when it does not.

Per-turn LLM round trips (glm-5.3-flash):
  greeting turn BEFORE this gate: agent(1) + composer(1) = 2 calls
  greeting turn AFTER:            agent(1)                    = 1 call
"""
from __future__ import annotations

import asyncio
import json

import app.api.v1.message as runner_mod
import app.db.repository as repo
from app.api.v1.message import _run
from app.services.dialogue import AgentTurnText
from tests.test_runner_flow import stub_io, valid_payload


async def _async(v):
    return v


def _contract(reply, intent="small_talk", entities=None):
    return json.dumps({
        "schema_version": "k2.dialogue.v4", "reply": reply,
        "turn": {"intent": intent}, "confidence": 0.9, "ambiguous": [],
        "confirmation": {"intent": "none"}, "selection": {"kind": "none", "rank": None},
        "entities": entities or {}, "operation_proposal": {"type": "none", "requested": False},
        "escalate": None,
    }, ensure_ascii=False)


def test_composer_skipped_when_primary_draft_passes_grounding(monkeypatch):
    stub_io(monkeypatch)

    compose_calls = []
    audit_entries = []

    async def spy_compose(context):
        compose_calls.append(context)
        return {"reply": "رد الكومبوزر", "evidence_ids": ["patient.current_message"],
                "missing_information": [], "unsupported_claims": [],
                "grounding_status": "supported", "raw_output": "{}"}

    async def spy_audit(entry):
        audit_entries.append(entry)
        return {}

    monkeypatch.setattr(runner_mod.dialogue, "compose_patient_reply", spy_compose)
    monkeypatch.setattr(repo, "log_agent_audit_entry", spy_audit)

    # a clean small-talk draft: no dates/times/numbers to fabricate
    monkeypatch.setattr(runner_mod.dialogue, "call_primary_model_with_tool",
                        lambda um, context: _async(AgentTurnText(
                            _contract("أهلاً بيك 🌸 كيف أقدر أساعدك؟"), llm_calls=1)))

    r = asyncio.run(_run(valid_payload(message_text="أهلا"), {}))
    assert r["reply_text"] == "أهلاً بيك 🌸 كيف أقدر أساعدك؟"
    assert compose_calls == [], "the composer must be skipped on a grounded primary draft"
    assert audit_entries[-1]["reply_composer"]["composer_skipped"] is True
    assert audit_entries[-1]["reply_composer"]["origin"] == "primary_grounded"


def test_tool_turn_ungrounded_draft_falls_through_to_composer(monkeypatch):
    stub_io(monkeypatch)

    compose_calls = []

    async def spy_compose(context):
        compose_calls.append(context)
        return {"reply": "عنوان العيادة في شارع الملك فهد 🌸",
                "evidence_ids": ["tool.0.Search_Clinic_FAQ"], "missing_information": [],
                "unsupported_claims": [], "grounding_status": "supported",
                "raw_output": "{}"}

    # the tool TURN: the draft ignores the tool's answer entirely (bare greeting)
    turn = AgentTurnText(_contract("أهلاً بيك 🌸"), tool_events=[{
        "name": "Search_Clinic_FAQ", "arguments": {"query": "العنوان"},
        "result": {"results": [{"content": "شارع الملك فهد"}], "count": 1},
    }], llm_calls=2)
    monkeypatch.setattr(runner_mod.dialogue, "call_primary_model_with_tool",
                        lambda um, context: _async(turn))
    monkeypatch.setattr(runner_mod.dialogue, "compose_patient_reply", spy_compose)

    r = asyncio.run(_run(valid_payload(message_text="عنوان العيادة فين؟"), {}))
    assert len(compose_calls) == 1, "a tool-turn draft that ignored the data must be composed"
    assert r["reply_text"] == "عنوان العيادة في شارع الملك فهد 🌸"


def test_tool_turn_grounded_draft_skips_the_composer(monkeypatch):
    stub_io(monkeypatch)

    compose_calls = []

    async def spy_compose(context):
        compose_calls.append(context)
        return {"reply": "composed", "evidence_ids": [], "missing_information": [],
                "unsupported_claims": [], "grounding_status": "supported", "raw_output": "{}"}

    # the draft ECHOES the tool's value — it passes and ships without the composer
    turn = AgentTurnText(_contract("عنواننا شارع الملك فهد 🌸"), tool_events=[{
        "name": "Search_Clinic_FAQ", "arguments": {"query": "العنوان"},
        "result": {"results": [{"content": "شارع الملك فهد"}], "count": 1},
    }], llm_calls=2)
    monkeypatch.setattr(runner_mod.dialogue, "call_primary_model_with_tool",
                        lambda um, context: _async(turn))
    monkeypatch.setattr(runner_mod.dialogue, "compose_patient_reply", spy_compose)

    r = asyncio.run(_run(valid_payload(message_text="عنوان العيادة فين؟"), {}))
    assert compose_calls == [], "a draft that echoes the tool value ships directly"
    assert "شارع الملك فهد" in r["reply_text"]
