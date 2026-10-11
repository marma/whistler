"""Library log levels (whistler/logsetup.py).

The kubernetes client logs every HTTP response body at DEBUG — including the
bodies of Secrets. `WHISTLER_LOG_LEVEL=DEBUG` and `kopf run --verbose` both
set the *root* level, so asking for Whistler's own debug output used to turn
that on as a side effect: hundreds of lines a second, with per-user VM access
keys among them.
"""
import logging

from whistler.logsetup import CHATTY_LIBRARY_LOGGERS, quiet_chatty_libraries


def _levels():
    return {n: logging.getLogger(n).getEffectiveLevel()
            for n in CHATTY_LIBRARY_LOGGERS}


def test_app_debug_does_not_drag_the_libraries_down(monkeypatch):
    monkeypatch.delenv("WHISTLER_LIB_LOG_LEVEL", raising=False)
    assert quiet_chatty_libraries("DEBUG") == "INFO"
    assert all(lvl == logging.INFO for lvl in _levels().values())


def test_a_quieter_app_level_is_respected(monkeypatch):
    monkeypatch.delenv("WHISTLER_LIB_LOG_LEVEL", raising=False)
    quiet_chatty_libraries("WARNING")
    assert all(lvl == logging.WARNING for lvl in _levels().values())


def test_the_libraries_can_still_be_asked_for_by_name(monkeypatch):
    """The output is genuinely useful when you want it — the objection is to
    getting it by accident."""
    monkeypatch.setenv("WHISTLER_LIB_LOG_LEVEL", "debug")
    assert quiet_chatty_libraries("INFO") == "DEBUG"
    assert all(lvl == logging.DEBUG for lvl in _levels().values())


def test_asyncssh_is_left_alone(monkeypatch):
    """Per-connection, not per-poll — and it is how you read a handshake."""
    assert "asyncssh" not in CHATTY_LIBRARY_LOGGERS


def test_the_operator_level_reaches_kopf(monkeypatch):
    """`kopf run --verbose` sets the root logger; operator.logLevel has to
    win over it, not just over Whistler's own loggers."""
    from whistler.logsetup import configure_operator_logging
    monkeypatch.delenv("WHISTLER_LIB_LOG_LEVEL", raising=False)
    root = logging.getLogger()
    before = root.level
    try:
        root.setLevel(logging.DEBUG)
        configure_operator_logging("error")
        assert logging.getLogger("kopf.objects").getEffectiveLevel() == logging.ERROR
        assert logging.getLogger("whistler.operator").getEffectiveLevel() == logging.ERROR
    finally:
        root.setLevel(before)
        logging.getLogger("whistler").setLevel(logging.NOTSET)


def _kopf_records(level, message):
    from whistler.logsetup import configure_operator_logging
    configure_operator_logging(level)
    seen = []

    class _Keep(logging.Handler):
        def emit(self, record):
            seen.append(record)

    objects = logging.getLogger("kopf.objects")
    h = _Keep()
    objects.addHandler(h)
    try:
        objects.info(message)
    finally:
        objects.removeHandler(h)
        logging.getLogger().setLevel(logging.WARNING)
        logging.getLogger("whistler").setLevel(logging.NOTSET)
    return seen


def test_a_routine_timer_success_is_not_an_info_line():
    assert _kopf_records("INFO", "Timer 'session_phase_timer' succeeded.") == []


def test_a_routine_timer_success_is_still_there_at_debug():
    [record] = _kopf_records("DEBUG", "Timer 'session_phase_timer' succeeded.")
    assert record.levelno == logging.DEBUG


def test_other_kopf_info_lines_are_untouched():
    [record] = _kopf_records("INFO", "Handler 'access_changed_fn' succeeded.")
    assert record.levelno == logging.INFO
