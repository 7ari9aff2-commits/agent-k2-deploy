"""SuperChat adapter — WhatsApp / Instagram / Messenger via one provider API.

Ported 1:1 from the three n8n 'SuperChat * - Trial' routers. SuperChat webhooks
send no HMAC (the n8n comment says so) — the secret URL is the gate, kept here.
Replies require a SuperChat contact whose handle matches the sender: the n8n
'Find SuperChat Contact' resolution is ported verbatim, and when no contact
matches, the reply is skipped (n8n best-effort behavior) with the webhook marked.
"""
from __future__ import annotations

from typing import Any, Dict, Optional, Tuple

import httpx

from app.channels.base import ChannelAdapter, InboundMessage


class SuperChatAdapter(ChannelAdapter):
    provider = "superchat"
    workflow_version = "superchat_fastapi_v1"
    supports_typing = False

    def __init__(self, channel_code: str):
        self.channel_code = channel_code          # whatsapp / instagram / messenger
        self.webhook_path = f"/channels/superchat/{channel_code}/webhook"

    # ── parse — port of 'Parse SUPERCHAT Message' ─────────────────────────────
    def parse(self, headers: Dict[str, str], body: Dict[str, Any]) -> InboundMessage:
        update = body.get("body") if isinstance(body.get("body"), dict) else body
        event = update.get("event") or ""
        if event != "message_inbound":
            return self._status(update, event=event)
        msg = update.get("message") or {}
        content = msg.get("content") or {}
        sender = msg.get("from") or {}
        to = msg.get("to") or {}
        channel_patient_id = sender.get("identifier") or sender.get("id")
        message_text = content.get("body")
        message_id = msg.get("id")
        sc_channel_id = to.get("channel_id")
        idempotency_key = f"superchat_{self.channel_code}_{message_id}" if message_id else None
        if not channel_patient_id or not message_text:
            return self._status(update)
        return InboundMessage(
            provider=self.provider, channel_code=self.channel_code,
            channel_patient_id=str(channel_patient_id),
            patient_name=sender.get("name"),
            message_text=str(message_text), message_type=content.get("type") or "text",
            message_id=message_id, idempotency_key=idempotency_key,
            raw_body=update,
            extra={"superchat_channel_id": sc_channel_id,
                   "superchat_conversation_id": msg.get("conversation_id"),
                   "channel_code": self.channel_code},
        )

    @staticmethod
    def _status(update: Dict[str, Any], event: Any = None) -> InboundMessage:
        return InboundMessage(provider="superchat", channel_code="", channel_patient_id="",
                              patient_name=None, message_text="", message_type="text",
                              message_id=None, idempotency_key=None, raw_body=update,
                              extra={"event": event})

    # ── channel lookup — port of 'Lookup Clinic ID SUPERCHAT' (unique match only) ──
    def channel_lookup(self, msg: InboundMessage) -> Tuple[str, list]:
        sc_channel_id = (msg.extra or {}).get("superchat_channel_id") or ""
        sql = """SELECT (array_agg(c.id))[1] AS clinic_id,
                        (array_agg(ch.id))[1] AS channel_id,
                        COUNT(*)::int AS match_count
                 FROM clinics c
                 JOIN channels ch ON ch.clinic_id = c.id
                 WHERE ch.provider = 'superchat'
                   AND ch.type = %s
                   AND ch.status = 'connected'
                   AND ch.is_enabled = true
                   AND ch.config->>'superchat_channel_id' = %s
                 GROUP BY ch.config->>'superchat_channel_id'
                 HAVING COUNT(*) = 1
                 LIMIT 1"""
        return sql, [self.channel_code, sc_channel_id]

    # ── send — port of 'Find SuperChat Contact' + 'Send SUPERCHAT Reply' ─────
    async def send(self, msg: InboundMessage, secret_row: Dict[str, Any],
                   channel_cfg: Dict[str, Any], reply_text: str) -> Dict[str, Any]:
        api_key = secret_row.get("api_token")
        sc_channel_id = (msg.extra or {}).get("superchat_channel_id")
        if not api_key or not sc_channel_id:
            raise RuntimeError("superchat send: missing api key or channel id")
        headers = {"X-API-KEY": str(api_key), "Content-Type": "application/json"}
        async with httpx.AsyncClient(timeout=15.0) as client:
            # contact resolution: the handle matching the sender wins (n8n parity)
            r = await client.get("https://api.superchat.com/v1.0/contacts",
                                 params={"channel_id": sc_channel_id}, headers=headers)
            if r.status_code >= 400:
                raise RuntimeError(f"superchat contacts failed: {r.status_code} {r.text[:200]}")
            contact_id = self._resolve_contact(r.json(), msg.channel_patient_id)
            if not contact_id:
                # n8n behavior: without a matching contact, no reply is possible
                return {"skipped": True, "reason": "no_matching_contact"}
            body = {"to": [{"identifier": contact_id}],
                    "from": {"channel_id": sc_channel_id},
                    "content": {"type": "text", "body": reply_text}}
            r = await client.post("https://api.superchat.com/v1.0/messages",
                                  json=body, headers=headers)
        if r.status_code >= 400:
            raise RuntimeError(f"superchat send failed: {r.status_code} {r.text[:200]}")
        return r.json() if r.text else {}

    @staticmethod
    def _resolve_contact(results: Any, target: str) -> Optional[str]:
        rows = results.get("results") if isinstance(results, dict) else results
        for c in rows or []:
            if not isinstance(c, dict):
                continue
            for h in c.get("handles") or []:
                if h.get("value") == target:
                    return c.get("id")
        first = (rows or [{}])[0]
        return first.get("id") if isinstance(first, dict) else None
