import asyncio
import logging
from typing import Optional

import asyncpg

from app.core.config import settings

logger = logging.getLogger(__name__)


async def _init_connection(conn: asyncpg.Connection) -> None:
    """Text-format codecs for temporal types: accepts ISO strings AND datetime objects on
    bind, returns ISO strings on fetch. Central fix for asyncpg's strict client-side typing
    (the ported n8n SQL binds $n::date / timestamptz parameters from JSON-ish payloads)."""
    def _enc_date(v):
        return v if isinstance(v, str) else str(v)

    def _enc_ts(v):
        return v.isoformat() if hasattr(v, "isoformat") else (v if isinstance(v, str) else str(v))

    await conn.set_type_codec("date", schema="pg_catalog", encoder=_enc_date,
                              decoder=lambda v: str(v), format="text")
    await conn.set_type_codec("timestamp", schema="pg_catalog", encoder=_enc_ts,
                              decoder=lambda v: str(v), format="text")
    await conn.set_type_codec("timestamptz", schema="pg_catalog", encoder=_enc_ts,
                              decoder=lambda v: str(v), format="text")


class DatabasePool:
    """Single asyncpg pool shared by all repositories (n8n used one Postgres credential)."""

    def __init__(self) -> None:
        self.pool: Optional[asyncpg.Pool] = None
        self._lock = asyncio.Lock()

    async def get_pool(self) -> asyncpg.Pool:
        # Locked: two concurrent first callers used to race create_pool and leak a pool.
        if self.pool is None:
            async with self._lock:
                if self.pool is None:
                    kwargs = dict(
                        dsn=settings.DATABASE_URL,
                        min_size=settings.DB_POOL_MIN,
                        max_size=settings.DB_POOL_MAX,
                        command_timeout=30,
                        init=_init_connection,
                    )
                    if settings.DB_SSL:
                        import ssl

                        ctx = ssl.create_default_context()
                        ctx.check_hostname = False
                        ctx.verify_mode = ssl.CERT_NONE
                        kwargs["ssl"] = ctx
                    self.pool = await asyncpg.create_pool(**kwargs)
        return self.pool

    async def close(self) -> None:
        if self.pool is not None:
            await self.pool.close()
            self.pool = None


db_pool = DatabasePool()


async def get_pool() -> asyncpg.Pool:
    return await db_pool.get_pool()
