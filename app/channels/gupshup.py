"""Gupshup WhatsApp adapter — ported 1:1 from 'WhatsApp Production - Gupshup'.

Honesty note (kept from the n8n code comment): Gupshup enterprise webhooks may
send X-Gupshup-Auth but no HMAC; the n8n gate was app-id sanity + the secret URL.
Same gate here. The sending apikey comes from get_channel_secret_db (api_token).
"""
from __future__ import annotations

from typing import Any, Dict, Tuple

import httpx

from app.channels.base import ChannelAdapter, InboundMessage


class GupshupWhatsAppAdapter(ChannelAdapter):
    provider = "gupshup"
    workflow_version = "gupshup_wa_fastapi_v1"
    supports_typing = False
    channel_code = "whatsapp"

    # ── parse — port of 'Parse GUPSHUP Message' ───────────────────────────────
    def parse(self, headers: Dict[str, str], body: Dict[str, Any]) -> InboundMessage:
        update = body.get("body") if isinstance(body.get("body"), dict) else body
        if update.get("type") != "message":
            return self._status(update, event=update.get("type"))
        p = update.get("payload") or {}
        sender = p.get("sender") or {}
        inner = p.get("payload") or {}
        channel_patient_id = p.get("source") or sender.get("phone")
        message_text = inner.get("text") or (inner.get("text") if p.get("type") == "text" else None)
        message_id = p.get("id")
        app_id = update.get("app")
        idempotency_key = (f"gupshup_wa_{app_id}_{message_id}"
                           if app_id and message_id else None)
        if not channel_patient_id or not message_text:
            return self._status(update)
        return InboundMessage(
            provider=self.provider, channel_code=self.channel_code,
            channel_patient_id=str(channel_patient_id),
            patient_name=sender.get("name"),
            message_text=str(message_text), message_type=p.get("type") or "text",
            message_id=message_id, idempotency_key=idempotency_key,
            raw_body=update,
            extra={"provider_app_id": app_id, "channel_code": self.channel_code},
        )

    @staticmethod
    def _status(update: Dict[str, Any], event: Any = None) -> InboundMessage:
        return InboundMessage(provider="gupshup", channel_code="whatsapp",
                              channel_patient_id="", patient_name=None, message_text="",
                              message_type="text", message_id=None, idempotency_key=None,
                              raw_body=update, extra={"event": event})

    # ── authenticity — app-id sanity gate, exactly as in n8n ──────────────────
    def verify_signature(self, headers: Dict[str, str], raw_body: bytes,
                         secret_row: Dict[str, Any]) -> bool:
        app_id = ""
        try:
            import json as _json
            update = _json.loads(raw_body.decode("utf-8")) if raw_body else {}
            app_id = str(update.get("app") or "").lower()
        except Exception:
            return False
        return app_id == "meruna2" or app_id == "gupshup" or "meruna" in app_id

    # ── channel lookup — port of 'Lookup Clinic ID GUPSHUP' ───────────────────
    def channel_lookup(self, msg: InboundMessage) -> Tuple[str, list]:
        app_id = str((msg.extra or {}).get("provider_app_id") or "meruna2")
        sql = """SELECT c.id AS clinic_id, ch.id AS channel_id,
                        ch.config->>'gupshup_source_number' AS gupshup_source_number
                 FROM clinics c
                 JOIN channels ch ON ch.clinic_id = c.id
                 WHERE ch.provider = 'gupshup'
                   AND ch.type = 'whatsapp'
                   AND ch.status = 'connected'
                   AND ch.is_enabled = true
                   AND c.deleted_at IS NULL
                   AND ch.deleted_at IS NULL
                   AND ch.config->>'gupshup_app_id' = %s
                 LIMIT 1"""
        return sql, [app_id]

    # ── send — port of 'Send GUPSHUP Reply' (form-urlencoded) ─────────────────
    async def send(self, msg: InboundMessage, secret_row: Dict[str, Any],
                   channel_cfg: Dict[str, Any], reply_text: str) -> Dict[str, Any]:
        apikey = secret_row.get("api_token")
        if not apikey:
            raise RuntimeError("gupshup send: missing apikey (channel secret api_token)")
        source = (channel_cfg or {}).get("gupshup_source_number") or "201107731857"
        body = {
            "channel": "whatsapp",
            "source": str(source),
            "destination": msg.channel_patient_id,
            "message": _json_compact({"type": "text", "text": reply_text}),
        }
        async with httpx.AsyncClient(timeout=15.0) as client:
            r = await client.post("https://api.gupshup.io/wa/api/v1/msg",
                                  data=body, headers={"apikey": str(apikey)})
        if r.status_code >= 400:
            raise RuntimeError(f"gupshup send failed: {r.status_code} {r.text[:200]}")
        return r.json() if r.text else {}


def _json_compact(obj: Dict[str, Any]) -> str:
    import json
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))
