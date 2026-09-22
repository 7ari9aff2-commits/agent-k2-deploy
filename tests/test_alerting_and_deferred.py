"""Alerting bridge + deferred worker ports — regression tests (2026-09-23)."""
import json

from app.core import alerting
from app.services import deferred_worker


# ── alerting ─────────────────────────────────────────────────────────────────


def test_report_noop_when_unconfigured(monkeypatch):
    alerting._COOLDOWN.clear()
    monkeypatch.setattr(alerting.settings, "TELEGRAM_ALERT_BOT_TOKEN", "")
    alerting.report("test.kind", "hello")       # must not raise, must not schedule
    assert True


def test_cooldown_blocks_second_alert(monkeypatch):
    alerting._COOLDOWN.clear()
    monkeypatch.setattr(alerting.settings, "TELEGRAM_ALERT_BOT_TOKEN", "tok")
    monkeypatch.setattr(alerting.settings, "TELEGRAM_ALERT_CHAT_ID", "123")
    sent = []

    class FakeClient:
        def __init__(self, *a, **k):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

        async def post(self, url, **k):
            sent.append(k["json"])
            class R:
                status_code = 200
                text = "{}"

                def json(self):
                    return {}
            return R()

    monkeypatch.setattr(alerting.httpx, "AsyncClient", FakeClient)
    alerting.report("dup.kind", "first")
    alerting.report("dup.kind", "second")       # same kind inside cooldown → dropped
    import asyncio
    asyncio.get_event_loop_policy().new_event_loop().run_until_complete(_drain())
    assert len(sent) == 1
    alerting._COOLDOWN.clear()


async def _drain():
    import asyncio
    pending = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    for t in pending:
        await t


def test_report_always_swallows_errors(monkeypatch):
    alerting._COOLDOWN.clear()
    monkeypatch.setattr(alerting.settings, "TELEGRAM_ALERT_BOT_TOKEN", "tok")
    monkeypatch.setattr(alerting.settings, "TELEGRAM_ALERT_CHAT_ID", "123")

    class BoomClient:
        def __init__(self, *a, **k):
            pass

        async def __aenter__(self):
            raise RuntimeError("network down")

        async def __aexit__(self, *a):
            return False

    monkeypatch.setattr(alerting.httpx, "AsyncClient", BoomClient)
    alerting.report("boom.kind", "x", cooldown=False)   # must not raise
    assert True


# ── deferred worker ──────────────────────────────────────────────────────────

BATCH = {
    "batch_id": "b-111",
    "clinic_id": "cl-1", "patient_id": "pa-1", "conversation_id": "co-1",
    "channel_type": "whatsapp", "channel_id": "ch-1",
    "merged_message_text": "رسالة أولى\nرسالة تانية",
    "message_items": [{"message_id": "m1", "received_at": "2026-09-23T10:00:00Z"},
                      {"message_id": "m2"}],
    "claim_token": "tok-1", "attempts": 1, "priority": False,
    "conversation_lease_token": "lease-1",
}


def test_core_payload_matches_n8n_worker_contract():
    p = deferred_worker._build_core_payload(BATCH)
    assert p["source_event_id"] == "k2-deferred-b-111"
    assert p["operation_id"] == "k2-deferred-b-111"
    assert p["metadata"]["k2_deferred_replay"] is True
    assert p["metadata"]["k2_deferred_item_count"] == 2
    assert p["message_text"].count("\n") == 1
    assert p["_worker"]["claim_token"] == "tok-1"


def test_message_text_capped_at_6000():
    b = dict(BATCH, merged_message_text="x" * 9000)
    assert len(deferred_worker._build_core_payload(b)["message_text"]) == 6000


def test_delivery_validation_matches_n8n_rules():
    ok_uuid = "ok:123e4567-e89b-12d3-a456-426614174000"
    assert deferred_worker._validate_delivery(ok_uuid, False) is True
    assert deferred_worker._validate_delivery("skipped: already_delivered", False) is True
    assert deferred_worker._validate_delivery("anything", True) is True
    assert deferred_worker._validate_delivery("unexpected response", False) is False
    assert deferred_worker._validate_delivery({"data": ok_uuid}, False) is True
