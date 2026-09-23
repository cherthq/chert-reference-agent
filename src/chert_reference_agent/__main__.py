"""Explicit service entrypoint. Importing the package never opens a connection."""
import logging
import os

from aiohttp import web

from .config import Config
from .http import create_app
from .livekit_adapter import LiveKitAdapter
from .service import Service


def main() -> None:
    # SDK/HTTP exceptions may contain URLs or participant material. Operational
    # evidence comes only from our allowlisted logger, never arbitrary SDK text.
    logging.disable(logging.CRITICAL)
    try:
        config = Config.from_env()
        port = int(os.environ.get("PORT", "8080"))
        if not 1 <= port <= 65535:
            raise ValueError("invalid port")
    except Exception:
        raise SystemExit("configuration_invalid") from None

    # Construct the SDK inside aiohttp's running event loop.
    async def application():
        adapter = LiveKitAdapter(
            config.livekit_url, config.livekit_api_key, config.livekit_api_secret,
            cleanup_step_timeout=min(1.0, config.cleanup_timeout_seconds / 4),
        )
        return create_app(Service(config, adapter))

    try:
        web.run_app(application(), host=os.environ.get("BIND_HOST", "127.0.0.1"),
                    port=port, access_log=None, print=None)
    except Exception:
        raise SystemExit("service_runtime_failed") from None


if __name__ == "__main__":
    main()
