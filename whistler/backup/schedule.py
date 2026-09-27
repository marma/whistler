"""When to back up, what to keep, and when to hold off (design/backup.md,
"Scheduling and retention" and "The first-login offer"). Pure: every rule
takes ``now`` and the list of backups on the volume, so each is a unit test.

The rules that make repeated reinstalls harmless live here:

- **Retention only prunes this install's scheduled backups.** Manual,
  uninstall, pre-restore and uploaded backups, and anything from another
  install, are an admin's to delete. Nothing a reinstall does can age out
  the backup it came from.
- **The schedule pauses while a restore is on offer.** A fresh install that
  backed itself up on schedule would, under retention, eventually delete the
  backups it should have restored.
"""

import datetime
import re
from typing import Any, Dict, List, Optional

TRIGGERS = ("manual", "scheduled", "uninstall", "pre-restore", "uploaded")
MODES = ("off", "interval", "daily")

DECISION_PENDING = "pending"
DECISION_RESTORED = "restored"
DECISION_DECLINED = "declined"
# Nothing from another install was on the volume, so there was nothing to
# offer: recorded so a later backup of THIS install is never offered back.
DECISION_FRESH = "fresh"

# whistler-backup-<YYYYmmdd-HHMMSS>-<trigger>[.<n>].tar.gz — the only names
# the service reads, writes or deletes, which is also its path validation.
FILENAME_RE = re.compile(
    r"^whistler-backup-(\d{8}-\d{6})-(" + "|".join(map(re.escape, TRIGGERS))
    + r")(?:\.(\d{1,4}))?\.tar\.gz$")

DEFAULT_SETTINGS = {"mode": "daily", "hours": 24, "at": "03:00", "retain": 14}


def parse_filename(name: str) -> Optional[Dict[str, Any]]:
    """``{"stamp": datetime, "trigger": str}`` for a backup file name, or
    None for anything else."""
    m = FILENAME_RE.match(name or "")
    if not m:
        return None
    try:
        stamp = datetime.datetime.strptime(m.group(1), "%Y%m%d-%H%M%S") \
            .replace(tzinfo=datetime.timezone.utc)
    except ValueError:
        return None
    return {"stamp": stamp, "trigger": m.group(2)}


def filename_for(created: datetime.datetime, trigger: str, taken=()) -> str:
    """A name for a new backup, unique among ``taken``."""
    if trigger not in TRIGGERS:
        raise ValueError(f"unknown trigger {trigger!r}")
    base = f"whistler-backup-{created:%Y%m%d-%H%M%S}-{trigger}"
    name, n = f"{base}.tar.gz", 1
    while name in taken:
        n += 1
        name = f"{base}.{n}.tar.gz"
    return name


def parse_settings(raw: Dict[str, Any],
                   defaults: Dict[str, Any] = None) -> Dict[str, Any]:
    """Validated settings; raises ValueError with the reason."""
    s = {**DEFAULT_SETTINGS, **(defaults or {}), **(raw or {})}
    if s["mode"] not in MODES:
        raise ValueError(f"mode must be one of {', '.join(MODES)}")
    try:
        s["hours"] = int(s["hours"])
        s["retain"] = int(s["retain"])
    except (TypeError, ValueError):
        raise ValueError("hours and retain must be whole numbers")
    if not 1 <= s["hours"] <= 24 * 31:
        raise ValueError("hours must be between 1 and 744")
    if not 1 <= s["retain"] <= 1000:
        raise ValueError("retain must be between 1 and 1000")
    if not re.fullmatch(r"([01]\d|2[0-3]):[0-5]\d", str(s["at"])):
        raise ValueError("at must be HH:MM (UTC)")
    return {k: s[k] for k in DEFAULT_SETTINGS}


def next_run(settings: Dict[str, Any], last: Optional[datetime.datetime],
             now: datetime.datetime) -> Optional[datetime.datetime]:
    """When the next scheduled backup is due (possibly already, i.e. <=
    ``now``), or None when the schedule is off. ``last`` is the last
    scheduled attempt, successful or skipped as unchanged."""
    mode = settings["mode"]
    if mode == "off":
        return None
    if mode == "interval":
        return now if last is None else \
            last + datetime.timedelta(hours=settings["hours"])
    hh, mm = map(int, settings["at"].split(":"))
    slot = now.replace(hour=hh, minute=mm, second=0, microsecond=0)
    if slot > now:
        slot -= datetime.timedelta(days=1)       # the most recent slot
    if last is None or last < slot:
        return slot                              # missed it: due now
    return slot + datetime.timedelta(days=1)


def is_due(settings, last, now) -> bool:
    when = next_run(settings, last, now)
    return when is not None and when <= now


def prunable(entries: List[Dict[str, Any]], install_id: str,
             retain: int) -> List[str]:
    """Backups retention may delete: this install's scheduled ones beyond
    the newest ``retain``. Nothing else, ever."""
    mine = [e for e in entries if e.get("trigger") == "scheduled"
            and install_id and e.get("installId") == install_id]
    mine.sort(key=lambda e: e["createdAt"], reverse=True)
    return [e["file"] for e in mine[retain:]]


def from_other_installs(entries: List[Dict[str, Any]],
                        install_id: str) -> List[Dict[str, Any]]:
    """What the first-login offer offers, newest first. A backup with no
    install id (made by the CLI and uploaded) counts as another install's."""
    others = [e for e in entries if e.get("installId") != install_id]
    return sorted(others, key=lambda e: e["createdAt"], reverse=True)


def offer_state(decision: str, entries, install_id) -> str:
    """``offer`` (pending and something to restore: schedule paused),
    ``fresh`` (pending with nothing to offer: record that and carry on), or
    ``settled``."""
    if decision != DECISION_PENDING:
        return "settled"
    return "offer" if from_other_installs(entries, install_id) else "fresh"


def last_scheduled(entries, install_id) -> Optional[datetime.datetime]:
    times = [e["createdAt"] for e in entries
             if e.get("trigger") == "scheduled"
             and e.get("installId") == install_id]
    return max(times) if times else None


def stale(settings, last_success: Optional[datetime.datetime],
          last_failed: bool, now: datetime.datetime) -> Optional[str]:
    """Why the dashboard should warn, or None: a backup job that stopped
    quietly is the usual way backups fail."""
    if settings["mode"] == "off":
        return "Scheduled backups are off."
    if last_failed:
        return "The last backup attempt failed."
    period = datetime.timedelta(hours=settings["hours"]) \
        if settings["mode"] == "interval" else datetime.timedelta(days=1)
    if last_success is None:
        return None      # a fresh install: nothing has had time to fail
    if now - last_success > 2 * period:
        return (f"No successful backup since "
                f"{last_success:%Y-%m-%d %H:%M} UTC.")
    return None
