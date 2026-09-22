"""Meta WhatsApp Cloud API adapter — ready for direct-Meta cutover.

The owner has not decided the WhatsApp provider yet (Gupshup vs Meta direct vs
SuperChat). This adapter makes the decision cheap: create the channel row with
provider='meta', point Meta's webhook at /channels/meta/whatsapp, set the phone
number id + token in the channel config/secret, and the same pipeline runs.

Inbound: Meta Cloud API payload {object:'whatsapp_business_account', entry:[…]}.
Signature: X-Hub-Signature-256 (HMAC-SHA256 of the raw body with the app secret).
Outbound: POST graph.facebook.com/v21.0/{phone_number_id}/messages.
Typing: mark-as-read with typing_indicator (≤25s) — supported.
"""
from __future__ import annotations

import hashlib
import hmac
import json
from typing import Any, Dict, Tuple

import httpx

from app.channels.base import ChannelAdapter, InboundMessage


class MetaWhatsAppAdapter(ChannelAdapter):
    provider = "meta"
    workflow_version = "meta_wa_fastapi_v1"
    supports_typing = True
    channel_code = "whatsapp"

    # ── parse — Meta Cloud API webhook shape ──────────────────────────────────
    def parse(self, headers: Dict[str, str], body: Dict[str, Any]) -> InboundMessage:
        update = body.get("body") if isinstance(body.get("body"), dict) else body
        entry = (update.get("entry") or [{}])[0]
        change = ((entry.get("changes") or [{}])[0]).get("value") or {}
        messages = change.get("messages") or []
        if not messages:
            # statuses (sent/delivered/read) and errors are status updates
            return self._status(update)
        m = messages[0]
        sender = (m.get("from") or "")
        text = ((m.get("text") or {}).get("body")
                or (m.get("caption") if isinstance(m.get("caption"), str) else None) or "")
        message_id = m.get("id")
        if not sender or not text:
            return self._status(update)
        return InboundMessage(
            provider=self.provider, channel_code=self.channel_code,
            channel_patient_id=str(sender), patient_name=None,
            message_text=str(text), message_type=m.get("type") or "text",
            message_id=message_id,
            idempotency_key=f"meta_wa_{message_id}" if message_id else None,
            raw_body=update,
            extra={"phone_number_id": change.get("metadata", {}).get("phone_number_id")},
            reply_to={"phone_number_id": change.get("metadata", {}).get("phone_number_id"),
                      "to": str(sender)},
        )

    @staticmethod
    def _status(update: Dict[str, Any]) -> InboundMessage:
        return InboundMessage(provider="meta", channel_code="whatsapp", channel_patient_id="",
                              patient_name=None, message_text="", message_type="text",
                              message_id=None, idempotency_key=None, raw_body=update,
                              extra={"event": "status"})

    # ── verify — X-Hub-Signature-256 against the Meta app secret (from DB secret row) ──
    def verify_signature(self, headers: Dict[str, str], raw_body: bytes,
                         secret_row: Dict[str, Any]) -> bool:
        app_secret = secret_row.get("signing_secret")
        if not app_secret:
            return False
        lower = {k.lower(): v for k, v in (headers or {}).items()}
        received = lower.get("x-hub-signature-256", "")
        expected = "sha256=" + hmac.new(str(app_secret).encode(), raw_body, hashlib.sha256).hexdigest()
        return hmac.compare_digest(received, expected)

    # ── channel lookup — by the phone number id Meta sends in every webhook ───
    def channel_lookup(self, msg: InboundMessage) -> Tuple[str, list]:
        phone_number_id = (msg.extra or {}).get("phone_number_id") or ""
        sql = """SELECT c.id AS clinic_id, ch.id AS channel_id, ch.config
                 FROM clinics c
                 JOIN channels ch ON ch.clinic_id = c.id
                 WHERE ch.provider = 'meta'
                   AND ch.type = 'whatsapp'
                   AND ch.status = 'connected'
                   AND ch.is_enabled = true
                   AND c.deleted_at IS NULL
                   AND ch.deleted_at IS NULL
                   AND ch.config->>'meta_phone_number_id' = $1
                 LIMIT 1"""
        return sql, [phone_number_id]

    # ── send — Cloud API text message; token via channel secret (api_token) ───
    async def send(self, msg: InboundMessage, secret_row: Dict[str, Any],
                   channel_cfg: Dict[str, Any], reply_text: str) -> Dict[str, Any]:
        token = secret_row.get("api_token")
        phone_number_id = (msg.reply_to or {}).get("phone_number_id") \
            or (channel_cfg or {}).get("meta_phone_number_id")
        to = (msg.reply_to or {}).get("to") or msg.channel_patient_id
        if not token or not phone_number_id:
            raise RuntimeError("meta send: missing token or phone_number_id")
        async with httpx.AsyncClient(timeout=20.0) as client:
            r = await client.post(
                f"https://graph.facebook.com/v21.0/{phone_number_id}/messages",
                json={"messaging_product": "whatsapp", "to": to,
                      "type": "text", "text": {"body": reply_text}},
                headers={"Authorization": f"Bearer {token}"})
        if r.status_code >= 400:
            raise RuntimeError(f"meta send failed: {r.status_code} {r.text[:200]}")
        return r.json()

    async def send_typing(self, msg: InboundMessage, secret_row: Dict[str, Any],
                          channel_cfg: Dict[str, Any]) -> bool:
        token = secret_row.get("api_token")
        phone_number_id = (msg.reply_to or {}).get("phone_number_id")
        message_id = msg.message_id
        if not token or not phone_number_id or not message_id:
            return False
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                r = await client.post(
                    f"https://graph.facebook.com/v21.0/{phone_number_id}/messages",
                    json={"messaging_product": "whatsapp", "status": "read",
                          "message_id": message_id, "typing_indicator": {"type": "text"}},
                    headers={"Authorization": f"Bearer {token}"})
            return r.status_code == 200
        except Exception:
            return False


def _json_dumps(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))
