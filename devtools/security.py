"""Authentication runs before any development cloud dependency is constructed."""

import hmac
from ipaddress import ip_address

from fastapi import Depends, HTTPException, Request
from fastapi.security import APIKeyHeader

from devtools.config import DevConfig, endpoints_enabled

token_header = APIKeyHeader(name="X-Dev-Token", auto_error=False)


def authorize(request: Request, token: str | None = Depends(token_header)) -> None:
    if not endpoints_enabled():
        raise HTTPException(status_code=404, detail="Not found")
    try:
        local = request.client is not None and ip_address(request.client.host).is_loopback
    except ValueError:
        local = False
    if not local:
        raise HTTPException(status_code=403, detail="Development tools require a loopback client")
    try:
        config = DevConfig.from_environment()
    except ValueError:
        raise HTTPException(
            status_code=503, detail="Development tools are not configured"
        ) from None
    if token is None or not hmac.compare_digest(
        token.encode("utf-8"), config.token.get_secret_value().encode("utf-8")
    ):
        raise HTTPException(status_code=401, detail="Development authorization required")
