"""Entry point.

  python -m fileuploader            # API + workers + relay + sweeper in one process
  python -m fileuploader api        # API only
  python -m fileuploader worker     # workers + outbox relay + sweeper only

API and worker processes share state only through SQLite, so they scale
independently and any process can die mid-request.
"""
from __future__ import annotations

import argparse
import asyncio
import signal

from . import db
from .worker import run_workers


def main() -> None:
    p = argparse.ArgumentParser(prog="fileuploader")
    p.add_argument("role", nargs="?", default="all", choices=["all", "api", "worker"])
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=8000)
    args = p.parse_args()

    db.init_db()
    if args.role == "worker":
        async def run() -> None:
            stop = asyncio.Event()
            loop = asyncio.get_running_loop()
            for sig in (signal.SIGINT, signal.SIGTERM):
                loop.add_signal_handler(sig, stop.set)
            await run_workers(stop)
        asyncio.run(run())
        return

    import uvicorn

    from .api import create_app
    uvicorn.run(create_app(run_workers=args.role == "all"), host=args.host, port=args.port, log_level="warning")


if __name__ == "__main__":
    main()
