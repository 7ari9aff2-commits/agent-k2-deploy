"""Telegram adapter — the native Bot API channel (fixed provider, as the owner decided).

Ported 1:1 from the n8n 'Telegram Reply Nodes' router (live-verified 2026-09-22).
Improvement over n8n: the webhook secret is verified via
X-Telegram-Bot-Api-Secret-Token (n8n's trigger had no secret — the URL was the gate).
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import httpx

from app.channels.base import ChannelAdapter, InboundMessage, ChannelResolution

TELEGRAM_WEBHOOK_SECRET = "k2tgw8h20260922"   # set at setWebhook; rotate via setWebhook only


class TelegramAdapter(ChannelAdapter):
    provider = "telegram"
    workflow_version = "telegram_fastapi_v1"
    supports_typing = True

    # ── parse — port of 'Parse Telegram Message' ─────────────────────────────
    def parse(self, headers: Dict[str, str], body: Dict[str, Any]) -> InboundMessage:
        update = body.get("body") if isinstance(body.get("body"), dict) else body
        is_edit = bool(update.get("edited_message") or update.get("edited_channel_post"))
        message = (update.get("channel_post") or update.get("edited_channel_post")
                   or update.get("message") or update.get("edited_message"))
        if not message:
            return self._status(update, event="telegram.update")
        chat = message.get("chat") or {}
        sender = message.get("from") or message.get("sender_chat") or {}
        # Bot-originated messages and edits never re-enter the agent loop (self-trigger guard).
        if sender.get("is_bot") is True or is_edit:
            return self._status(update, event="message.edited_ignored" if is_edit else "bot_message_ignored")
        text = message.get("text") or message.get("caption") or ""
        chat_id = chat.get("id")
        sender_id = sender.get("id") or chat_id
        message_id = message.get("message_id")
        source_event_id = update.get("update_id") or message_id
        idempotency_key = (f"telegram:{chat_id}:{source_event_id}"
                           if chat_id is not None and source_event_id is not None else None)
        received_at = (datetime.fromtimestamp(int(message["date"]), tz=timezone.utc).isoformat()
                       if message.get("date") else datetime.now(timezone.utc).isoformat())
        message_type = ("media" if any(k in message for k in ("photo", "video", "document", "audio"))
                        else "text")
        return InboundMessage(
            provider=self.provider, channel_code="telegram",
            channel_patient_id=str(sender_id) if sender_id is not None else "",
            patient_name=None,
            message_text=str(text or ""), message_type=message_type,
            message_id=message_id, idempotency_key=idempotency_key,
            raw_body=update,
            extra={"telegram_chat_id": chat_id, "telegram_message_id": message_id,
                   "message_type": message_type,
                   "channel_username": f"@{chat['username']}" if chat.get("username") else None},
            reply_to={"chat_id": chat_id},
        )

    @staticmethod
    def _status(update: Dict[str, Any], event: str) -> InboundMessage:
        return InboundMessage(provider="telegram", channel_code="telegram",
                              channel_patient_id="", patient_name=None, message_text="",
                              message_type="text", message_id=None, idempotency_key=None,
                              raw_body=update, extra={"event": event})

    # ── webhook authenticity — the secret token header ────────────────────────
    def verify_signature(self, headers: Dict[str, str], raw_body: bytes,
                         secret_row: Dict[str, Any]) -> bool:
        given = (headers.get("x-telegram-bot-api-secret-token")
                 or headers.get("X-Telegram-Bot-Api-Secret-Token") or "")
        return given == TELEGRAM_WEBHOOK_SECRET

    # ── channel lookup — port of 'Lookup Telegram Clinic' ─────────────────────
    def channel_lookup(self, msg: InboundMessage) -> Tuple[str, list]:
        chat_id = str((msg.extra or {}).get("telegram_chat_id"))
        sql = """SELECT ch.clinic_id, ch.id AS channel_id, ch.config
                 FROM public.channels ch
                 WHERE ch.type = 'telegram'
                   AND ch.status = 'connected'
                   AND ch.is_enabled = true
                   AND ch.deleted_at IS NULL
                   AND NOT COALESCE((ch.config->>'test_only')::boolean, false)
                 ORDER BY CASE WHEN ch.config->>'telegram_chat_id' = %s THEN 0 ELSE 1 END,
                          ch.created_at
                 LIMIT 1"""
        return sql, [chat_id]

    # ── send — Bot API sendMessage (token from the channel row config) ────────
    async def send(self, msg: InboundMessage, secret_row: Dict[str, Any],
                   channel_cfg: Dict[str, Any], reply_text: str) -> Dict[str, Any]:
        token = self._bot_token(channel_cfg)
        chat_id = (msg.reply_to or {}).get("chat_id") or (msg.extra or {}).get("telegram_chat_id")
        if not token or chat_id is None:
            raise RuntimeError("telegram send: missing bot token or chat id")
        async with httpx.AsyncClient(timeout=20.0) as client:
            r = await client.post(
                f"https://api.telegram.org/bot{token}/sendMessage",
                json={"chat_id": chat_id, "text": reply_text, "parse_mode": "HTML",
                      "link_preview_options": {"is_disabled": True}})
        if r.status_code >= 400:
            raise RuntimeError(f"telegram sendMessage failed: {r.status_code} {r.text[:200]}")
        return r.json()

    async def send_typing(self, msg: InboundMessage, secret_row: Dict[str, Any],
                          channel_cfg: Dict[str, Any]) -> bool:
        token = self._bot_token(channel_cfg)
        chat_id = (msg.reply_to or {}).get("chat_id") or (msg.extra or {}).get("telegram_chat_id")
        if not token or chat_id is None:
            return False
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                r = await client.post(f"https://api.telegram.org/bot{token}/sendChatAction",
                                      json={"chat_id": chat_id, "action": "typing"})
            return r.status_code == 200 and bool(r.json().get("result"))
        except Exception:
            return False

    @staticmethod
    def _bot_token(channel_cfg: Dict[str, Any]) -> Optional[str]:
        cfg = channel_cfg if isinstance(channel_cfg, dict) else {}
        token = cfg.get("botToken")
        return str(token) if token else None
