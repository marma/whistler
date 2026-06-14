"""Portal entrypoint: ``python -m whistler.portal``.

Mirrors whistler.server's startup — configure logging, build one
KubeConfigManager, and serve the aiohttp app. KubeConfigManager loads in-cluster
config (falling back to the local kubeconfig), so the same process works both
in-cluster and as a host process for integration tests.
"""
import logging
import os
import sys

from aiohttp import web

from whistler.config import KubeConfigManager
from whistler.portal.app import build_app


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

    port = int(os.environ.get("PORTAL_PORT", "8080"))
    logger.info(f"Starting Whistler portal on :{port} "
                f"(guacd {os.environ.get('GUACD_HOST', 'whistler-guacd')}:"
                f"{os.environ.get('GUACD_PORT', '4822')})")
    web.run_app(build_app(config_manager), host="0.0.0.0", port=port, print=None)


if __name__ == "__main__":
    main()
