"""Backup and restore of Whistler's state (design/backup.md, Phase 2).

``archive``  the file: build and read it (pure, no cluster).
``export``   what goes into it, read from the cluster.
``restore``  putting it back: verify, preview, apply, read back.

The CLI (``python -m whistler.backup``) and, later, the backup service are
thin wrappers over these.
"""


class BackupError(Exception):
    """A backup that cannot be read, or cannot be restored here. The message
    is written for the admin who has to act on it."""
