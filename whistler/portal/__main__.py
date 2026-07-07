"""Portal entrypoint: ``python -m whistler.portal``.

Runs two apps side-by-side in the same event loop:

  * **Management app** (FastAPI, port MANAGEMENT_PORT, default 8081)
    Admin + user web UI backed by Jinja2 templates.

  * **Viewer app** (aiohttp, port PORTAL_PORT, default 8080)
    Desktop viewer (Selkies 2.x websockets) + web terminal for sessions.

Both share the same KubeConfigManager instance.
Set WHISTLER_AUTH_ALLOW_ANY=true and WHISTLER_AUTH_ALLOW_ADMIN=true for local dev.
"""
import asyncio
import logging
import os
import sys

import uvicorn
from aiohttp import web

from whistler.config import KubeConfigManager
from whistler.portal.app import build_app
from whistler.portal.management import build_management_app


def main():
    log_level = os.environ.get("WHISTLER_LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        stream=sys.stderr,
        force=True,
    )
    logger = logging.getLogger("whistler.portal")

    kubeconfig = os.environ.get("KUBECONFIG")
    config_manager = KubeConfigManager(kubeconfig=kubeconfig)

    viewer_port = int(os.environ.get("PORTAL_PORT", "8080"))
    mgmt_port = int(os.environ.get("MANAGEMENT_PORT", "8081"))

    logger.info(
        f"Starting Whistler portal — "
        f"management :{mgmt_port}, "
        f"viewer :{viewer_port}"
    )

    asyncio.run(_run_both(config_manager, viewer_port, mgmt_port, log_level, logger))


async def _run_both(config_manager, viewer_port: int, mgmt_port: int, log_level: str, logger):
    # --- aiohttp viewer app (desktop viewer + web terminal) ---
    aio_app = build_app(config_manager)
    runner  = web.AppRunner(aio_app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", viewer_port)
    await site.start()
    logger.info(f"Viewer app listening on :{viewer_port}")

    # --- FastAPI management app ---
    fastapi_app = build_management_app(config_manager)
    uv_config   = uvicorn.Config(
        fastapi_app,
        host="0.0.0.0",
        port=mgmt_port,
        log_level=log_level.lower(),
        loop="none",       # reuse the running asyncio loop
    )
    server = uvicorn.Server(uv_config)
    logger.info(f"Management UI listening on :{mgmt_port}")

    try:
        await server.serve()
    finally:
        await runner.cleanup()


if __name__ == "__main__":
    main()
