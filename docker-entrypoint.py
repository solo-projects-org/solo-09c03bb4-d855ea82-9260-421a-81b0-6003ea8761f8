#!/usr/bin/env python3
"""Container entrypoint: fix the shared data volume, then drop privileges.

A Docker named volume mounted at /data hides the ownership set when the image
was built, so the unprivileged app user cannot create the SQLite file (and its
WAL/lock files). Start as root, hand the data directory to ``appuser`` once,
then exec the real command as ``appuser``. When the container is already
running unprivileged the ownership step is skipped.
"""
from __future__ import annotations

import os
import pwd
import sys

APP_USER = "appuser"


def _chown_tree(path: str, uid: int, gid: int) -> None:
    try:
        os.chown(path, uid, gid)
    except FileNotFoundError:
        return
    for root, dirs, files in os.walk(path):
        for name in dirs + files:
            try:
                os.chown(os.path.join(root, name), uid, gid)
            except FileNotFoundError:
                pass  # another instance replaced/removed it concurrently


def main() -> None:
    if len(sys.argv) < 2:
        raise SystemExit("usage: docker-entrypoint.py <command> [args...]")

    if os.geteuid() == 0:
        db_path = os.environ.get("RAIN_DB_PATH", "/data/rainfall.db")
        data_dir = os.path.dirname(os.path.abspath(db_path))
        os.makedirs(data_dir, exist_ok=True)
        try:
            account = pwd.getpwnam(APP_USER)
        except KeyError:
            account = None
        if account is not None:
            _chown_tree(data_dir, account.pw_uid, account.pw_gid)
            os.setgroups([])
            os.setgid(account.pw_gid)
            os.setuid(account.pw_uid)

    os.execvp(sys.argv[1], sys.argv[1:])


if __name__ == "__main__":
    main()
