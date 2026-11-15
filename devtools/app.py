"""Local composition: retain production HTTP routes without starting its telemetry."""

from fastapi import FastAPI
from fastapi.routing import APIRoute

from devtools.config import endpoints_enabled


def create_app(base: FastAPI | None = None) -> FastAPI:
    if base is None:
        from services.apify_receiver import app as base
    app = FastAPI(
        title="Job Harvester local development",
        middleware=list(base.user_middleware),
    )
    # Reuse handlers unchanged; local startup does not resolve production secrets or
    # initialize telemetry. The normal production entry point remains untouched.
    app.router.routes.extend(route for route in base.router.routes if isinstance(route, APIRoute))
    if endpoints_enabled():
        from devtools.config import DevConfig
        from devtools.router import router

        DevConfig.from_environment()  # Enabled but missing credentials fails startup.
        app.include_router(router)
    return app
