"""Owner alerting bridge — FastAPI failures ping the owner's Telegram directly.

Why: the n8n K2 Error Monitor only sees n8n workflow failures (errorTrigger).
Once the critical path lives in FastAPI, its failures are INVISIBLE to n8n.
This bridge closes that gap: any pipeline/channel exception reaches the owner's
Telegram chat fire-and-forget, with a per-kind cooldown so a recurring failure
cannot turn into an alert storm.

Failures of the alerting itself are swallowed by design — alerting must never
take the reply path down with it.
"""
from __future__ import annotations

import asyncio
import logging
import time
from typing import Optional

import httpx

from app.core.config import settings

logger = logging.getLogger(__name__)

ALERT_COOLDOWN_SECONDS = 600   # same kind of failure → max one alert per 10 minutes
_COOLDOWN: dict = {}


def _configured() -> bool:
    return bool(settings.TELEGRAM_ALERT_BOT_TOKEN and settings.TELEGRAM_ALERT_CHAT_ID)


def _cooldown_open(kind: str) -> bool:
    now = time.monotonic()
    last = _COOLDOWN.get(kind, 0.0)
    if now - last < ALERT_COOLDOWN_SECONDS:
        return False
    _COOLDOWN[kind] = now
    return True


async def _deliver(text: str) -> None:
    url = f"https://api.telegram.org/bot{settings.TELEGRAM_ALERT_BOT_TOKEN}/sendMessage"
    async with httpx.AsyncClient(timeout=10.0) as client:
        r = await client.post(url, json={
            "chat_id": settings.TELEGRAM_ALERT_CHAT_ID,
            "text": text[:3500],
            "parse_mode": "HTML",
            "link_preview_options": {"is_disabled": True},
        })
        if r.status_code >= 400:
            logger.warning("alerting.deliver_failed %s %s", r.status_code, r.text[:120])


def report(kind: str, text: str, cooldown: bool = True) -> None:
    """Fire-and-forget owner alert. Never raises, never blocks the caller."""
    if not _configured():
        return
    if cooldown and not _cooldown_open(kind):
        return
    body = f"⚠️ <b>K2 {kind}</b>\n{text}"

    async def _run():
        try:
            await _deliver(body)
        except Exception:
            logger.warning("alerting.delivery_error", exc_info=True)

    try:
        asyncio.get_running_loop().create_task(_run())
    except RuntimeError:
        # no running loop (rare sync context) — deliver synchronously, still guarded
        try:
            asyncio.run(_run())
        except Exception:
            logger.warning("alerting.delivery_error_sync", exc_info=True)


def report_exception(kind: str, exc: BaseException, detail: str = "",
                     cooldown: bool = True) -> None:
    report(kind, f"{detail}\n<code>{type(exc).__name__}: {exc}</code>", cooldown)
