"""Logging levels for the libraries Whistler runs on.

Shared by both processes (the SSH server and the operator) and deliberately
tiny — importing whistler.server into the operator would drag Textual and
asyncssh in with it.

The problem this solves: `WHISTLER_LOG_LEVEL` / `kopf --verbose` are about
*Whistler's* logs, but both set the root level, and the kubernetes client logs
every HTTP **response body** at DEBUG. That is several hundred lines a second
from the pod and session watches — enough to bury the one line you were
looking for in a `skaffold dev` pane — and among them the bodies of every
Secret read: the portal keys and the per-user VM access keys, in plaintext, in
a log that gets pasted into issues.

So library logs are pinned no lower than INFO unless someone asks for them by
name with `WHISTLER_LIB_LOG_LEVEL=DEBUG`. asyncssh is deliberately *not* on
the list: its DEBUG is per-connection rather than per-poll, and it is how you
diagnose a handshake.
"""

import logging
import os

CHATTY_LIBRARY_LOGGERS = ("kubernetes", "urllib3")


def quiet_chatty_libraries(app_level: str = "INFO") -> str:
    """Pin the noisy libraries' level. Returns the level applied."""
    level = os.environ.get("WHISTLER_LIB_LOG_LEVEL", "").strip().upper()
    if not level:
        # Never noisier than the app asked for, and never below INFO.
        level = "INFO" if (app_level or "").upper() in (
            "DEBUG", "NOTSET", "") else app_level.upper()
    for name in CHATTY_LIBRARY_LOGGERS:
        logging.getLogger(name).setLevel(level)
    return level
