"""Thikaa Instagram adapter — ported 1:1 from 'Instagram Channel (provider-agnostic)'.

The n8n verify node carried a hardcoded signing secret ('ab41d1f1…') — a v1
shortcut. Here the secret comes from get_channel_secret_db (signing_secret),
with the same x-thikaa-signature header and 'sha256=' prefix handling, and the
same timing-safe comparison. Sending goes through the tenant messaging API
(tenant 'hamedadel' — kept verbatim from production; parameterize later if the
owner signs new tenants).
"""
from __future__ import annotations

import hashlib
import hmac
import json
from typing import Any, Dict, Tuple

import httpx

from app.channels.base import ChannelAdapter, InboundMessage

THIKAA_TENANT = "hamedadel"


class ThikaaInstagramAdapter(ChannelAdapter):
    provider = "thikaa"
    workflow_version = "thikaa_ig_fastapi_v1"
    supports_typing = False
    channel_code = "instagram"

    # ── parse — port of 'Parse Instagram Thikaa Message1' ─────────────────────
    def parse(self, headers: Dict[str, str], body: Dict[str, Any]) -> InboundMessage:
        update = body.get("body") if isinstance(body.get("body"), dict) else body
        if update.get("event") != "message.received":
            return self._status(update, event=update.get("event"))
        data = update.get("data") or {}
        channel_patient_id = data.get("from")
        message_text = data.get("text")
        message_id = data.get("message_id")
        instance_id = update.get("instance_id")
        idempotency_key = (f"thikaa_ig_{instance_id}_{message_id}"
                           if instance_id and message_id else None)
        if not channel_patient_id or not message_text:
            return self._status(update)
        return InboundMessage(
            provider=self.provider, channel_code=self.channel_code,
            channel_patient_id=str(channel_patient_id).lstrip("+"),
            patient_name=data.get("from_user_name"),
            message_text=str(message_text),
            message_type="media" if (data.get("attachments") or []) else "text",
            message_id=message_id, idempotency_key=idempotency_key,
            raw_body=update,
            extra={"thikaa_instance_id": instance_id,
                   "channel_code": data.get("channel") or "ig"},
            reply_to={"to": str(channel_patient_id).lstrip("+")},
        )

    @staticmethod
    def _status(update: Dict[str, Any], event: Any = None) -> InboundMessage:
        return InboundMessage(provider="thikaa", channel_code="instagram",
                              channel_patient_id="", patient_name=None, message_text="",
                              message_type="text", message_id=None, idempotency_key=None,
                              raw_body=update, extra={"event": event})

    # ── verify — timing-safe HMAC over the RAW body, 'sha256=' prefix stripped ──
    def verify_signature(self, headers: Dict[str, str], raw_body: bytes,
                         secret_row: Dict[str, Any]) -> bool:
        signing_secret = secret_row.get("signing_secret")
        if not signing_secret:
            return False
        lower = {k.lower(): v for k, v in (headers or {}).items()}
        received = lower.get("x-thikaa-signature")
        if not received:
            return False
        if received.startswith("sha256="):
            received = received[len("sha256="):]
        expected = hmac.new(str(signing_secret).encode(), raw_body, hashlib.sha256).hexdigest()
        return hmac.compare_digest(str(received), expected)

    # ── channel lookup — by thikaa instance id (config column) ────────────────
    def channel_lookup(self, msg: InboundMessage) -> Tuple[str, list]:
        instance_id = (msg.extra or {}).get("thikaa_instance_id") or ""
        sql = """SELECT c.id AS clinic_id, ch.id AS channel_id, ch.config
                 FROM clinics c
                 JOIN channels ch ON ch.clinic_id = c.id
                 WHERE ch.provider = 'thikaa'
                   AND ch.type = 'instagram'
                   AND ch.status = 'connected'
                   AND ch.is_enabled = true
                   AND c.deleted_at IS NULL
                   AND ch.deleted_at IS NULL
                   AND ch.config->>'thikaa_instance_id' = $1
                 LIMIT 1"""
        return sql, [instance_id]

    # ── send — port of 'Send Instagram Thikaa Reply1' ──────────────────────────
    async def send(self, msg: InboundMessage, secret_row: Dict[str, Any],
                   channel_cfg: Dict[str, Any], reply_text: str) -> Dict[str, Any]:
        token = secret_row.get("api_token")
        to = (msg.reply_to or {}).get("to") or msg.channel_patient_id
        if not token:
            raise RuntimeError("thikaa send: missing api_token")
        body = {"channel": "ig", "to": str(to), "message": str(reply_text)}
        async with httpx.AsyncClient(timeout=15.0) as client:
            r = await client.post(
                f"https://thikaa.com/thik_tenant_{THIKAA_TENANT}/api/messaging/?action=send",
                json=body,
                headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"})
        if r.status_code >= 400:
            raise RuntimeError(f"thikaa send failed: {r.status_code} {r.text[:200]}")
        return r.json() if r.text else {}
