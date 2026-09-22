"""Channel adapter registry + FastAPI router.

One endpoint per provider webhook path. n8n stays deployed on ITS paths as the
rollback: cutover = pointing the provider webhook at /channels/.../webhook,
rollback = pointing it back at the n8n URL.
"""
from __future__ import annotations

import logging
from typing import Any, Dict

from fastapi import APIRouter, Request

from app.channels.base import handle_webhook
from app.channels.gupshup import GupshupWhatsAppAdapter
from app.channels.meta_whatsapp import MetaWhatsAppAdapter
from app.channels.superchat import SuperChatAdapter
from app.channels.telegram import TelegramAdapter
from app.channels.thikaa import ThikaaInstagramAdapter

logger = logging.getLogger(__name__)

ADAPTERS: Dict[str, Any] = {
    "telegram": TelegramAdapter(),
    "gupshup": GupshupWhatsAppAdapter(),
    "meta": MetaWhatsAppAdapter(),
    "superchat_whatsapp": SuperChatAdapter("whatsapp"),
    "superchat_instagram": SuperChatAdapter("instagram"),
    "superchat_messenger": SuperChatAdapter("messenger"),
    "thikaa_instagram": ThikaaInstagramAdapter(),
}

router = APIRouter(prefix="/channels", tags=["Channels"])


def _low(key: str) -> str:
    return key.lower()


@router.post("/telegram/webhook")
async def telegram_webhook(request: Request):
    adapter = ADAPTERS["telegram"]
    body = await request.json()
    status, payload = await handle_webhook(adapter, dict(request.headers), body,
                                           await request.body())
    return _json_response(status, payload)


@router.post("/gupshup/webhook")
async def gupshup_webhook(request: Request):
    adapter = ADAPTERS["gupshup"]
    body = await request.json()
    status, payload = await handle_webhook(adapter, dict(request.headers), body,
                                           await request.body())
    return _json_response(status, payload)


@router.post("/meta/whatsapp/webhook")
async def meta_whatsapp_webhook(request: Request):
    adapter = ADAPTERS["meta"]
    body = await request.json()
    status, payload = await handle_webhook(adapter, dict(request.headers), body,
                                           await request.body())
    return _json_response(status, payload)


@router.get("/meta/whatsapp/webhook")
async def meta_whatsapp_verify(request: Request):
    """Meta webhook subscription handshake (hub.challenge echo)."""
    hub_challenge = request.query_params.get("hub.challenge")
    verify_token = request.query_params.get("hub.verify_token")
    if hub_challenge is None:
        return _json_response(400, {"status": "error", "reason": "missing_hub_challenge"})
    return _plain_response(200, hub_challenge if verify_token else "")


@router.post("/superchat/{channel_code}/webhook")
async def superchat_webhook(channel_code: str, request: Request):
    key = f"superchat_{channel_code}"
    if key not in ADAPTERS:
        return _json_response(404, {"status": "error", "reason": "unknown_channel"})
    body = await request.json()
    status, payload = await handle_webhook(ADAPTERS[key], dict(request.headers), body,
                                           await request.body())
    return _json_response(status, payload)


@router.post("/thikaa/instagram/webhook")
async def thikaa_instagram_webhook(request: Request):
    body = await request.json()
    status, payload = await handle_webhook(ADAPTERS["thikaa_instagram"], dict(request.headers),
                                           body, await request.body())
    return _json_response(status, payload)


def _json_response(status: int, payload: Dict[str, Any]):
    from fastapi.responses import JSONResponse
    return JSONResponse(status_code=status, content=payload)


def _plain_response(status: int, text: str):
    from fastapi.responses import PlainTextResponse
    return PlainTextResponse(status_code=status, content=text)
