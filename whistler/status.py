"""The user-facing session states, in one place.

The operator, the pod lifecycle and KubeVirt each speak their own phase
vocabulary (`Ready`, `Booting`, `Importing`, a bare pod `Running`, nothing at
all); every surface that shows a session has to collapse those into the few
states a person actually decides on — is it up, is it coming up, is it off.

This lived in whistler/portal/management.py, which is the wrong home now that
the launcher decides the same thing: the TUI cannot import a module that pulls
in FastAPI and Jinja, and a second copy of the table is a second copy that
drifts. The portal still owns the *colours* it renders these with.
"""

# Raw phase (lower-cased) -> the state shown to a user. Anything unknown, and
# an absent phase, read as Stopped: no workload we can see means nothing to
# connect to, which is the safe reading for both the badge and the launcher.
STATUS_GROUPS = {
    "running":      "Running",
    "ready":        "Running",
    "pending":      "Pending",
    "initializing": "Starting",
    "provisioning": "Starting",
    "booting":      "Starting",
    "importing":    "Starting",
    "stopping":     "Stopping",
    "terminating":  "Stopping",
    "stopped":      "Stopped",
    "unknown":      "Stopped",
    "failed":       "Error",
}

# Fomantic UI label colours for the portal's badges.
GROUP_COLORS = {
    "Running":  "green",
    "Starting": "yellow",
    "Pending":  "blue",
    "Stopping": "orange",
    "Stopped":  "grey",
    "Error":    "red",
}


def status_group(status: str, ready: bool = True) -> str:
    """Collapse a raw pod/CR phase into one of the user-facing states. A
    "running" phase pod whose containers aren't all ready yet reads as
    Starting rather than Running."""
    s = (status or "").lower()
    if s == "running" and not ready:
        return "Starting"
    return STATUS_GROUPS.get(s, "Stopped")
