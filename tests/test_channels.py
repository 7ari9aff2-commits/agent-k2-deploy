"""Channel adapter layer — regression tests (2026-09-23).

The adapters are ports of the n8n routers (live-verified 2026-09-22). These
tests pin: payload parsing per provider, signature/authenticity gates, the K2
envelope shape and its deterministic serialization, and the reply extraction
with fallback — the exact behaviors production relied on.
"""
import hashlib
import hmac
import json

from app.channels import ADAPTERS
from app.channels.base import (_extract_conversation, _extract_patient_id,
                               _extract_reply, _handoff_suppressed, _json_compact)
from app.channels.gupshup import GupshupWhatsAppAdapter
from app.channels.superchat import SuperChatAdapter
from app.channels.telegram import TELEGRAM_WEBHOOK_SECRET, TelegramAdapter
from app.channels.thikaa import ThikaaInstagramAdapter

# ── registry completeness ─────────────────────────────────────────────────────


def test_registry_has_all_channels():
    assert set(ADAPTERS.keys()) == {
        "telegram", "gupshup", "meta",
        "superchat_whatsapp", "superchat_instagram", "superchat_messenger",
        "thikaa_instagram",
    }


# ── telegram ──────────────────────────────────────────────────────────────────

TG_UPDATE = {
    "update_id": 777,
    "message": {"message_id": 42, "date": 1790107000,
                "chat": {"id": 555, "type": "private"},
                "from": {"id": 555, "is_bot": False, "first_name": "Hamed"},
                "text": "السلام عليكم"},
}


def test_telegram_parse_real_message():
    msg = TelegramAdapter().parse({}, TG_UPDATE)
    assert msg.channel_patient_id == "555"
    assert msg.message_text == "السلام عليكم"
    assert msg.idempotency_key == "telegram:555:777"
    assert msg.is_status_update is False
    assert msg.reply_to["chat_id"] == 555


def test_telegram_parse_ignores_bot_and_edits():
    bot_update = {"update_id": 1, "message": {"message_id": 2, "chat": {"id": 5},
                                              "from": {"id": 5, "is_bot": True},
                                              "text": "hi"}}
    edit_update = {"update_id": 3, "edited_message": {"message_id": 4, "chat": {"id": 5},
                                                      "text": "edited"}}
    assert TelegramAdapter().parse({}, bot_update).is_status_update is True
    assert TelegramAdapter().parse({}, edit_update).is_status_update is True


def test_telegram_parse_status_update():
    assert TelegramAdapter().parse({}, {"update_id": 9, "my_chat_member": {}}).is_status_update is True


def test_telegram_secret_token_gate():
    a = TelegramAdapter()
    assert a.verify_signature({"X-Telegram-Bot-Api-Secret-Token": TELEGRAM_WEBHOOK_SECRET},
                              b"{}", {}) is True
    assert a.verify_signature({"X-Telegram-Bot-Api-Secret-Token": "wrong"}, b"{}", {}) is False
    assert a.verify_signature({}, b"{}", {}) is False


def test_telegram_channel_lookup_prefers_matching_chat():
    sql, params = TelegramAdapter().channel_lookup(TelegramAdapter().parse({}, TG_UPDATE))
    assert "test_only" in sql and params == ["555"]


# ── gupshup ───────────────────────────────────────────────────────────────────

GUPSHUP_EVENT = {
    "app": "meruna2", "timestamp": 1718007189549, "version": 2, "type": "message",
    "payload": {"id": "ABEG123", "source": "20112223334", "type": "text",
                "payload": {"text": "عايز احجز"},
                "sender": {"phone": "20112223334", "name": "Hamed"}},
}


def test_gupshup_parse_real_message():
    msg = GupshupWhatsAppAdapter().parse({}, GUPSHUP_EVENT)
    assert msg.channel_patient_id == "20112223334"
    assert msg.message_text == "عايز احجز"
    assert msg.idempotency_key == "gupshup_wa_meruna2_ABEG123"


def test_gupshup_parse_ignores_status_events():
    ev = dict(GUPSHUP_EVENT, type="delivered")
    assert GupshupWhatsAppAdapter().parse({}, ev).is_status_update is True


def test_gupshup_app_id_gate():
    a = GupshupWhatsAppAdapter()
    raw = json.dumps(GUPSHUP_EVENT).encode()
    assert a.verify_signature({}, raw, {}) is True
    bad = json.dumps(dict(GUPSHUP_EVENT, app="someone-else")).encode()
    assert a.verify_signature({}, bad, {}) is False


# ── superchat ─────────────────────────────────────────────────────────────────

SUPERCHAT_EVENT = {
    "id": "pe_1", "event": "message_inbound",
    "message": {"id": "ms_9", "status": "received",
                "content": {"body": "hello", "type": "text"},
                "direction": "inbound",
                "to": {"channel_id": "mc_abc"},
                "from": {"id": "ct_1", "identifier": "+20112223334", "name": "Hamed"},
                "conversation_id": "cv_1"},
}


def test_superchat_parse_real_message():
    msg = SuperChatAdapter("whatsapp").parse({}, SUPERCHAT_EVENT)
    assert msg.channel_patient_id == "+20112223334"
    assert msg.message_text == "hello"
    assert msg.extra["superchat_channel_id"] == "mc_abc"
    assert msg.idempotency_key == "superchat_whatsapp_ms_9"


def test_superchat_parse_ignores_outbound_events():
    ev = dict(SUPERCHAT_EVENT, event="message_sent")
    assert SuperChatAdapter("whatsapp").parse({}, ev).is_status_update is True


def test_superchat_contact_resolution():
    results = {"results": [
        {"id": "ct_A", "handles": [{"value": "+20000000000"}]},
        {"id": "ct_B", "handles": [{"value": "+20112223334"}]},
    ]}
    assert SuperChatAdapter._resolve_contact(results, "+20112223334") == "ct_B"
    # fallback: first contact (n8n best-effort parity)
    assert SuperChatAdapter._resolve_contact({"results": [{"id": "ct_A", "handles": []}]}, "x") == "ct_A"
    assert SuperChatAdapter._resolve_contact({"results": []}, "x") is None


# ── thikaa ────────────────────────────────────────────────────────────────────

THIKAA_EVENT = {
    "event": "message.received", "instance_id": "inst_33e3141f9df86b0a",
    "timestamp": 1790100000,
    "data": {"channel": "ig", "conversation_id": "c1", "message_id": "m-9",
             "from": "17895550001", "from_user_name": "Hamed", "text": "ازيك",
             "attachments": []},
}

THIKAA_SECRET = "ab41d1f152cf71f024498874e083d9c6b050c83fe8545aab"  # the n8n node's secret


def test_thikaa_parse_real_message():
    msg = ThikaaInstagramAdapter().parse({}, THIKAA_EVENT)
    assert msg.channel_patient_id == "17895550001"
    assert msg.message_text == "ازيك"
    assert msg.idempotency_key == "thikaa_ig_inst_33e3141f9df86b0a_m-9"


def test_thikaa_parse_ignores_other_events():
    ev = dict(THIKAA_EVENT, event="message.read")
    assert ThikaaInstagramAdapter().parse({}, ev).is_status_update is True


def test_thikaa_hmac_verify_timing_safe():
    a = ThikaaInstagramAdapter()
    raw = json.dumps(THIKAA_EVENT, ensure_ascii=False).encode()
    sig = "sha256=" + hmac.new(THIKAA_SECRET.encode(), raw, hashlib.sha256).hexdigest()
    assert a.verify_signature({"x-thikaa-signature": sig}, raw,
                              {"signing_secret": THIKAA_SECRET}) is True
    assert a.verify_signature({"x-thikaa-signature": "sha256=deadbeef"}, raw,
                              {"signing_secret": THIKAA_SECRET}) is False
    assert a.verify_signature({}, raw, {"signing_secret": THIKAA_SECRET}) is False
    assert a.verify_signature({"x-thikaa-signature": sig}, raw, {}) is False


# ── meta whatsapp ─────────────────────────────────────────────────────────────


def test_meta_parse_real_message():
    from app.channels.meta_whatsapp import MetaWhatsAppAdapter
    event = {"object": "whatsapp_business_account",
             "entry": [{"changes": [{"value": {
                 "metadata": {"phone_number_id": "PNID1"},
                 "messages": [{"from": "20112223334", "id": "wamid.X", "type": "text",
                               "text": {"body": "مساء الخير"}}]}}]}]}
    msg = MetaWhatsAppAdapter().parse({}, event)
    assert msg.channel_patient_id == "20112223334"
    assert msg.message_text == "مساء الخير"
    assert msg.extra["phone_number_id"] == "PNID1"
    # statuses are ignored
    statuses = {"object": "whatsapp_business_account",
                "entry": [{"changes": [{"value": {"statuses": [{"id": "wamid.X", "status": "read"}]}}]}]}
    assert MetaWhatsAppAdapter().parse({}, statuses).is_status_update is True


# ── shared pipeline pieces ────────────────────────────────────────────────────


def test_k2_serialization_is_deterministic():
    payload = {"b": 1, "a": "نص"}
    assert _json_compact(payload) == '{"b":1,"a":"نص"}'


def test_patient_id_extraction_shapes():
    assert _extract_patient_id({"data": [{"patient_id": "p1"}]}) == "p1"
    assert _extract_patient_id({"patient_id": "p2"}) == "p2"
    assert _extract_patient_id([{"patient_id": "p3"}]) == "p3"
    try:
        _extract_patient_id({"nope": 1})
        raise SystemExit("should have raised")
    except RuntimeError:
        pass


def test_conversation_extraction_shapes():
    cid, ch = _extract_conversation({"conversation_id": "c9", "channel_id": "ch9"})
    assert (cid, ch) == ("c9", "ch9")
    cid, ch = _extract_conversation({"data": [{"result": {"conversation_id": "c1"}}]})
    assert cid == "c1"
    cid, ch = _extract_conversation([{"get_or_create_channel_conversation": {"conversation_id": "c2"}}])
    assert cid == "c2"


def test_reply_extraction_precedence_and_fallback():
    assert _extract_reply({"reply_text": "الرد"}) == "الرد"
    assert _extract_reply({"response": "بديل"}) == "بديل"
    assert _extract_reply({}) is None


def test_handoff_suppressed_conditions():
    assert _handoff_suppressed({"suppress_reply": True}) is True
    assert _handoff_suppressed({"duplicate": True}) is True
    assert _handoff_suppressed({"message": "already_processed"}) is True
    assert _handoff_suppressed({"body": {"suppress_reply": True}}) is True
    assert _handoff_suppressed({"reply_text": "x"}) is False


def test_core_payload_contract_shape():
    from app.channels.base import ChannelResolution
    a = ADAPTERS["telegram"]
    msg = a.parse({}, TG_UPDATE)
    payload = a.core_payload(msg,
                             ChannelResolution("clinic-1", "channel-1"),
                             "patient-1", "conv-1")
    # same keys the n8n Build K2 Signed Core Envelope emitted
    assert set(payload.keys()) == {"clinic_id", "channel_type", "channel_provider",
                                   "channel_id", "patient_id", "conversation_id",
                                   "message_text", "message_id", "source_event_id", "metadata"}
    assert payload["source_event_id"] == "telegram:555:777"
