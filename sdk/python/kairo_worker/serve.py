"""Serve Dify workflow nodes with graphon's DSL node factory.

    python -m kairo_worker.serve --socket /path/to/worker.sock \\
        --actions dify.template-transform,dify.http-request [--concurrency 8]

For Dify itself, build a Worker with GraphonNodeRunner and Dify's node
factory instead.
"""

from __future__ import annotations

import argparse
import logging

from .graphon import GraphonNodeRunner, slim_factory
from .worker import Worker


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--socket", help="UNIX socket of the runtime")
    p.add_argument("--tcp", help="host:port of the runtime")
    p.add_argument("--actions", required=True, help="comma-separated actions, e.g. dify.template-transform")
    p.add_argument("--concurrency", type=int, default=8)
    p.add_argument("--name", default="graphon")
    a = p.parse_args()
    logging.basicConfig(level=logging.INFO)
    addr: str | tuple[str, int]
    if a.socket:
        addr = a.socket
    else:
        host, port = a.tcp.rsplit(":", 1)
        addr = (host, int(port))
    Worker(a.name, a.actions.split(","), GraphonNodeRunner(slim_factory()), a.concurrency).run(addr)


if __name__ == "__main__":
    main()
