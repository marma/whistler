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


class _DemoteRoutineTimers(logging.Filter):
    """kopf logs every successful timer run at INFO — for the 10s Session
    phase timer that is one line per session every 10 seconds, which is most
    of an INFO log. Treat it as the DEBUG line it is, which also keeps it off
    kopf's Event poster (it posts INFO and up)."""

    def filter(self, record):
        if record.levelno == logging.INFO:
            msg = record.getMessage()
            if msg.startswith("Timer ") and msg.endswith(" succeeded."):
                if not logging.getLogger(record.name).isEnabledFor(logging.DEBUG):
                    return False
                record.levelno, record.levelname = logging.DEBUG, "DEBUG"
        return True


def configure_operator_logging(level: str = "INFO") -> str:
    """Apply OPERATOR_LOG_LEVEL to the whole operator, kopf included.

    `kopf run` sets the root level from its own flags (`--verbose` → DEBUG)
    before any handler runs; setting only the `whistler` logger left kopf's
    per-object lines at whatever the command line said, whatever the chart's
    `operator.logLevel` was."""
    level = (level or "INFO").strip().upper()
    logging.getLogger().setLevel(level)
    logging.getLogger("whistler").setLevel(level)
    objects = logging.getLogger("kopf.objects")
    if not any(isinstance(f, _DemoteRoutineTimers) for f in objects.filters):
        objects.addFilter(_DemoteRoutineTimers())
    quiet_chatty_libraries(level)
    return level
