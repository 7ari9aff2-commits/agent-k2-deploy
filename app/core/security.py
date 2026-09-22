import hmac
import logging

from fastapi import Header, HTTPException, status

from app.core.config import settings

logger = logging.getLogger(__name__)


def verify_internal_token(
    x_k2_internal_token: str = Header(default="", alias="X-K2-Internal-Token"),
) -> bool:
    """Source node: Webhook - Incoming Message (n8n headerAuth, header name X-K2-Internal-Token)."""
    if not settings.K2_INTERNAL_TOKEN:
        logger.error("K2_INTERNAL_TOKEN is not configured")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="server token misconfigured")
    # compare_digest over bytes: the str/str form raises TypeError on non-ASCII header
    # values, turning a 401 into an unhandled 500.
    supplied = x_k2_internal_token.encode("utf-8", errors="replace")
    expected = settings.K2_INTERNAL_TOKEN.encode("utf-8")
    if not hmac.compare_digest(supplied, expected):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="unauthorized")
    return True


